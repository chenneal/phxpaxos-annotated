/*
Tencent is pleased to support the open source community by making 
PhxPaxos available.
Copyright (C) 2016 THL A29 Limited, a Tencent company. 
All rights reserved.

Licensed under the BSD 3-Clause License (the "License"); you may 
not use this file except in compliance with the License. You may 
obtain a copy of the License at

https://opensource.org/licenses/BSD-3-Clause

Unless required by applicable law or agreed to in writing, software 
distributed under the License is distributed on an "AS IS" basis, 
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or 
implied. See the License for the specific language governing 
permissions and limitations under the License.

See the AUTHORS file for names of contributors. 
*/

#include "proposer.h"
#include "learner.h"
#include "phxpaxos/sm.h"
#include "instance.h"

namespace phxpaxos
{

// 记住 proposalID 是从 1 开始的。
ProposerState :: ProposerState(const Config * poConfig)
{
    m_poConfig = (Config *)poConfig;
    m_llProposalID = 1;
    Init();
}

ProposerState :: ~ProposerState()
{
}

void ProposerState :: Init()
{
    m_llHighestOtherProposalID = 0;
    m_sValue.clear();
}

void ProposerState ::  SetStartProposalID(const uint64_t llProposalID)
{
    m_llProposalID = llProposalID;
}

void ProposerState :: NewPrepare()
{
    PLGHead("START ProposalID %lu HighestOther %lu MyNodeID %lu",
            m_llProposalID, m_llHighestOtherProposalID, m_poConfig->GetMyNodeID());
     
    // 再从新发起 proposal 时，我们要确保自己的 Id 大于所有已经被知晓的
    // 旧 ID 的最大值，如果相等，增一即可。
    // ID 增大的时候多半是与其余节点产生了冲突。
    // 这里不用担心 ID 重复的情况，因为 ballot ID 包含 nodeID 。
    uint64_t llMaxProposalID =
        m_llProposalID > m_llHighestOtherProposalID ? m_llProposalID : m_llHighestOtherProposalID;

    m_llProposalID = llMaxProposalID + 1;

    PLGHead("END New.ProposalID %lu", m_llProposalID);

}

void ProposerState :: AddPreAcceptValue(
        const BallotNumber & oOtherPreAcceptBallot, 
        const std::string & sOtherPreAcceptValue)
{
    PLGDebug("OtherPreAcceptID %lu OtherPreAcceptNodeID %lu HighestOtherPreAcceptID %lu "
            "HighestOtherPreAcceptNodeID %lu OtherPreAcceptValue %zu",
            oOtherPreAcceptBallot.m_llProposalID, oOtherPreAcceptBallot.m_llNodeID,
            m_oHighestOtherPreAcceptBallot.m_llProposalID, m_oHighestOtherPreAcceptBallot.m_llNodeID, 
            sOtherPreAcceptValue.size());

    if (oOtherPreAcceptBallot.isnull())
    {
        return;
    }

    // 更新最大的 ballot ID 值。
    if (oOtherPreAcceptBallot > m_oHighestOtherPreAcceptBallot)
    {
        m_oHighestOtherPreAcceptBallot = oOtherPreAcceptBallot;
        m_sValue = sOtherPreAcceptValue;
    }
}

const uint64_t ProposerState :: GetProposalID()
{
    return m_llProposalID;
}

const std::string & ProposerState :: GetValue()
{
    return m_sValue;
}

void ProposerState :: SetValue(const std::string & sValue)
{
    m_sValue = sValue;
}

void ProposerState :: SetOtherProposalID(const uint64_t llOtherProposalID)
{
    if (llOtherProposalID > m_llHighestOtherProposalID)
    {
        m_llHighestOtherProposalID = llOtherProposalID;
    }
}

void ProposerState :: ResetHighestOtherPreAcceptBallot()
{
    m_oHighestOtherPreAcceptBallot.reset();
}

////////////////////////////////////////////////////////////////

Proposer :: Proposer(
        const Config * poConfig, 
        const MsgTransport * poMsgTransport,
        const Instance * poInstance,
        const Learner * poLearner,
        const IOLoop * poIOLoop)
    : Base(poConfig, poMsgTransport, poInstance), m_oProposerState(poConfig), m_oMsgCounter(poConfig)
{
    m_poLearner = (Learner *)poLearner;
    m_poIOLoop = (IOLoop *)poIOLoop;
    
    m_bIsPreparing = false;
    m_bIsAccepting = false;

    m_bCanSkipPrepare = false;

    InitForNewPaxosInstance();

    m_iPrepareTimerID = 0;
    m_iAcceptTimerID = 0;
    m_llTimeoutInstanceID = 0;

    m_iLastPrepareTimeoutMs = m_poConfig->GetPrepareTimeoutMs();
    m_iLastAcceptTimeoutMs = m_poConfig->GetAcceptTimeoutMs();

    m_bWasRejectBySomeone = false;
}

Proposer :: ~Proposer()
{
}

void Proposer :: SetStartProposalID(const uint64_t llProposalID)
{
    m_oProposerState.SetStartProposalID(llProposalID);
}

void Proposer :: InitForNewPaxosInstance()
{
    m_oMsgCounter.StartNewRound();
    m_oProposerState.Init();

    ExitPrepare();
    ExitAccept();
}

bool Proposer :: IsWorking()
{
    return m_bIsPreparing || m_bIsAccepting;
}

int Proposer :: NewValue(const std::string & sValue)
{
    BP->GetProposerBP()->NewProposal(sValue);

    if (m_oProposerState.GetValue().size() == 0)
    {
        m_oProposerState.SetValue(sValue);
    }

    m_iLastPrepareTimeoutMs = START_PREPARE_TIMEOUTMS;
    m_iLastAcceptTimeoutMs = START_ACCEPT_TIMEOUTMS;

    // 这里直接做了 multi-paxos 的优化，去掉了 single-paxos 的第一阶段。
    if (m_bCanSkipPrepare && !m_bWasRejectBySomeone)
    {
        BP->GetProposerBP()->NewProposalSkipPrepare();

        PLGHead("skip prepare, directly start accept");
        Accept();
    }
    // 如果冲突了，要重新执行 prepare 阶段。
    else
    {
        //if not reject by someone, no need to increase ballot
        Prepare(m_bWasRejectBySomeone);
    }

    return 0;
}

void Proposer :: ExitPrepare()
{
    // 如果中途退出，说明已经超时了。
    if (m_bIsPreparing)
    {
        m_bIsPreparing = false;
        
        m_poIOLoop->RemoveTimer(m_iPrepareTimerID);
    }
}

void Proposer :: ExitAccept()
{
    if (m_bIsAccepting)
    {
        m_bIsAccepting = false;
        
        m_poIOLoop->RemoveTimer(m_iAcceptTimerID);
    }
}

void Proposer :: AddPrepareTimer(const int iTimeoutMs)
{
    // 如果旧 ID 的还在定时器 map 中，去掉就可，重置定时器 ID 为0。
    if (m_iPrepareTimerID > 0)
    {
        m_poIOLoop->RemoveTimer(m_iPrepareTimerID);
    }

    // 3.27 : 暂时认为如果如果传入的 timeout 有新的值，更新即可。
    if (iTimeoutMs > 0)
    {
        m_poIOLoop->AddTimer(
                iTimeoutMs,
                Timer_Proposer_Prepare_Timeout,
                m_iPrepareTimerID);
        return;
    }

    // 3.27 : 如果传入的值为 0 ，我们把 timeout 值定 m_iLastPrepareTimeoutMs 
    // 下次再遇到时直接乘以 2 减少超时的可能性。
    m_poIOLoop->AddTimer(
            m_iLastPrepareTimeoutMs,
            Timer_Proposer_Prepare_Timeout,
            m_iPrepareTimerID);

    m_llTimeoutInstanceID = GetInstanceID();

    PLGHead("timeoutms %d", m_iLastPrepareTimeoutMs);

    // 最新的 prepare timeout 默认值直接乘以 2，但是不能超过阈值。 
    m_iLastPrepareTimeoutMs *= 2;
    if (m_iLastPrepareTimeoutMs > MAX_PREPARE_TIMEOUTMS)
    {
        m_iLastPrepareTimeoutMs = MAX_PREPARE_TIMEOUTMS;
    }
}

void Proposer :: AddAcceptTimer(const int iTimeoutMs)
{
    if (m_iAcceptTimerID > 0)
    {
        m_poIOLoop->RemoveTimer(m_iAcceptTimerID);
    }

    if (iTimeoutMs > 0)
    {
        m_poIOLoop->AddTimer(
                iTimeoutMs,
                Timer_Proposer_Accept_Timeout,
                m_iAcceptTimerID);
        return;
    }

    m_poIOLoop->AddTimer(
            m_iLastAcceptTimeoutMs,
            Timer_Proposer_Accept_Timeout,
            m_iAcceptTimerID);

    m_llTimeoutInstanceID = GetInstanceID();
    
    PLGHead("timeoutms %d", m_iLastPrepareTimeoutMs);

    m_iLastAcceptTimeoutMs *= 2;
    if (m_iLastAcceptTimeoutMs > MAX_ACCEPT_TIMEOUTMS)
    {
        m_iLastAcceptTimeoutMs = MAX_ACCEPT_TIMEOUTMS;
    }
}

void Proposer :: Prepare(const bool bNeedNewBallot)
{
    PLGHead("START Now.InstanceID %lu MyNodeID %lu State.ProposalID %lu State.ValueLen %zu",
            GetInstanceID(), m_poConfig->GetMyNodeID(), m_oProposerState.GetProposalID(),
            m_oProposerState.GetValue().size());

    BP->GetProposerBP()->Prepare();
    m_oTimeStat.Point();
    
    ExitAccept();
    m_bIsPreparing = true;
    // 进入了这个函数，代表不能跳过一阶段。
    m_bCanSkipPrepare = false;
    m_bWasRejectBySomeone = false;

    // 重置最大值选项，方便冲突后新的 ballotID 的选定。
    m_oProposerState.ResetHighestOtherPreAcceptBallot();
    if (bNeedNewBallot)
    {
        m_oProposerState.NewPrepare();
    }

    PaxosMsg oPaxosMsg;
    oPaxosMsg.set_msgtype(MsgType_PaxosPrepare);
    oPaxosMsg.set_instanceid(GetInstanceID());
    oPaxosMsg.set_nodeid(m_poConfig->GetMyNodeID());
    oPaxosMsg.set_proposalid(m_oProposerState.GetProposalID());

    // 不管 accept 成功还是失败，我们都开始新的一轮计数。
    m_oMsgCounter.StartNewRound();

    // 将当前的 prepare 加入到定时器当中去。
    AddPrepareTimer();

    PLGHead("END OK");

    // 广播给所有的节点尝试 prepare 。
    BroadcastMessage(oPaxosMsg);
}

void Proposer :: OnPrepareReply(const PaxosMsg & oPaxosMsg)
{
    PLGHead("START Msg.ProposalID %lu State.ProposalID %lu Msg.from_nodeid %lu RejectByPromiseID %lu",
            oPaxosMsg.proposalid(), m_oProposerState.GetProposalID(), 
            oPaxosMsg.nodeid(), oPaxosMsg.rejectbypromiseid());

    BP->GetProposerBP()->OnPrepareReply();

    // 收到消息时发现已经不在 prepare 阶段了，直接忽略这个消息。
    if (!m_bIsPreparing)
    {
        BP->GetProposerBP()->OnPrepareReplyButNotPreparing();
        //PLGErr("Not preparing, skip this msg");
        return;
    }

    // 虽然正在 prepare 阶段，但是 proposeID 不一致，同样忽略。
    if (oPaxosMsg.proposalid() != m_oProposerState.GetProposalID())
    {
        BP->GetProposerBP()->OnPrepareReplyNotSameProposalIDMsg();
        //PLGErr("ProposalID not same, skip this msg");
        return;
    }

    // 统计回复的节点数量。
    m_oMsgCounter.AddReceive(oPaxosMsg.nodeid());

    if (oPaxosMsg.rejectbypromiseid() == 0)
    {
        BallotNumber oBallot(oPaxosMsg.preacceptid(), oPaxosMsg.preacceptnodeid());
        PLGDebug("[Promise] PreAcceptedID %lu PreAcceptedNodeID %lu ValueSize %zu", 
                oPaxosMsg.preacceptid(), oPaxosMsg.preacceptnodeid(), oPaxosMsg.value().size());
        // 统计赞成的节点数量。
        m_oMsgCounter.AddPromiseOrAccept(oPaxosMsg.nodeid());
        m_oProposerState.AddPreAcceptValue(oBallot, oPaxosMsg.value());
    }
    else
    {
        PLGDebug("[Reject] RejectByPromiseID %lu", oPaxosMsg.rejectbypromiseid());
        
        // 统计拒绝的节点数量。
        m_oMsgCounter.AddReject(oPaxosMsg.nodeid());
        m_bWasRejectBySomeone = true;
        m_oProposerState.SetOtherProposalID(oPaxosMsg.rejectbypromiseid());
    }

    // 超过半数赞同意味着本次 prepare 阶段成功。
    if (m_oMsgCounter.IsPassedOnThisRound())
    {
        int iUseTimeMs = m_oTimeStat.Point();
        BP->GetProposerBP()->PreparePass(iUseTimeMs);
        PLGImp("[Pass] start accept, usetime %dms", iUseTimeMs);

        // 3.21 : 下次再次运行 proposer 时，不需要再进行 prepare 阶段了。
        // 可能有人会问为什么要这样，因为在等待 accept 回复的过程中，
        // 当前线程会重新扔进 loop 中，再次唤醒需要一个标志位判断。
        m_bCanSkipPrepare = true;
        Accept();
    }
    // 3.21 : 收到大多数节点 reject 的消息或者已经收到了收到了所有节点的消息。
    // 设立一个随机的定时器，为的是与别的节点避免冲突。
    else if (m_oMsgCounter.IsRejectedOnThisRound()
            || m_oMsgCounter.IsAllReceiveOnThisRound())
    {
        BP->GetProposerBP()->PrepareNotPass();
        PLGImp("[Not Pass] wait 30ms and restart prepare");
        AddPrepareTimer(OtherUtils::FastRand() % 30 + 10);
    }

    PLGHead("END");
}

void Proposer :: OnExpiredPrepareReply(const PaxosMsg & oPaxosMsg)
{
    // 提升自己的 proposalID 值，这里是一个工程优化。
    if (oPaxosMsg.rejectbypromiseid() != 0)
    {
        PLGDebug("[Expired Prepare Reply Reject] RejectByPromiseID %lu", oPaxosMsg.rejectbypromiseid());
        m_bWasRejectBySomeone = true;
        m_oProposerState.SetOtherProposalID(oPaxosMsg.rejectbypromiseid());
    }
}

void Proposer :: Accept()
{
    PLGHead("START ProposalID %lu ValueSize %zu ValueLen %zu", 
            m_oProposerState.GetProposalID(), m_oProposerState.GetValue().size(), m_oProposerState.GetValue().size());

    BP->GetProposerBP()->Accept();
    m_oTimeStat.Point();

    // 已经到了 accept 状态，清除 prepare 阶段的标志位和定时器。
    ExitPrepare();
    m_bIsAccepting = true;
    
    PaxosMsg oPaxosMsg;
    oPaxosMsg.set_msgtype(MsgType_PaxosAccept);
    oPaxosMsg.set_instanceid(GetInstanceID());
    oPaxosMsg.set_nodeid(m_poConfig->GetMyNodeID());
    oPaxosMsg.set_proposalid(m_oProposerState.GetProposalID());
    oPaxosMsg.set_value(m_oProposerState.GetValue());
    oPaxosMsg.set_lastchecksum(GetLastChecksum());

    // 不管 accept 成功还是失败，我们都开始新的一轮计数。
    m_oMsgCounter.StartNewRound();

    AddAcceptTimer();

    PLGHead("END");

    // 3.27 : 发送给所有的节点尝试 accept ，自己最后再尝试，这样有什么意义么?
    BroadcastMessage(oPaxosMsg, BroadcastMessage_Type_RunSelf_Final);
}

void Proposer :: OnAcceptReply(const PaxosMsg & oPaxosMsg)
{
    PLGHead("START Msg.ProposalID %lu State.ProposalID %lu Msg.from_nodeid %lu RejectByPromiseID %lu",
            oPaxosMsg.proposalid(), m_oProposerState.GetProposalID(), 
            oPaxosMsg.nodeid(), oPaxosMsg.rejectbypromiseid());

    BP->GetProposerBP()->OnAcceptReply();

    // 和 onprepare 一样，忽略消息，下同。
    if (!m_bIsAccepting)
    {
        //PLGErr("Not proposing, skip this msg");
        BP->GetProposerBP()->OnAcceptReplyButNotAccepting();
        return;
    }

    if (oPaxosMsg.proposalid() != m_oProposerState.GetProposalID())
    {
        //PLGErr("ProposalID not same, skip this msg");
        BP->GetProposerBP()->OnAcceptReplyNotSameProposalIDMsg();
        return;
    }

    m_oMsgCounter.AddReceive(oPaxosMsg.nodeid());

    if (oPaxosMsg.rejectbypromiseid() == 0)
    {
        PLGDebug("[Accept]");
        m_oMsgCounter.AddPromiseOrAccept(oPaxosMsg.nodeid());
    }
    else
    {
        PLGDebug("[Reject]");
        m_oMsgCounter.AddReject(oPaxosMsg.nodeid());

        m_bWasRejectBySomeone = true;

        m_oProposerState.SetOtherProposalID(oPaxosMsg.rejectbypromiseid());
    }

    if (m_oMsgCounter.IsPassedOnThisRound())
    {
        int iUseTimeMs = m_oTimeStat.Point();
        BP->GetProposerBP()->AcceptPass(iUseTimeMs);
        PLGImp("[Pass] Start send learn, usetime %dms", iUseTimeMs);
        ExitAccept();
        m_poLearner->ProposerSendSuccess(GetInstanceID(), m_oProposerState.GetProposalID());
    }
    else if (m_oMsgCounter.IsRejectedOnThisRound()
            || m_oMsgCounter.IsAllReceiveOnThisRound())
    {
        BP->GetProposerBP()->AcceptNotPass();
        PLGImp("[Not pass] wait 30ms and Restart prepare");
        AddAcceptTimer(OtherUtils::FastRand() % 30 + 10);
    }

    PLGHead("END");
}

void Proposer :: OnExpiredAcceptReply(const PaxosMsg & oPaxosMsg)
{
    if (oPaxosMsg.rejectbypromiseid() != 0)
    {
        PLGDebug("[Expired Accept Reply Reject] RejectByPromiseID %lu", oPaxosMsg.rejectbypromiseid());
        m_bWasRejectBySomeone = true;
        m_oProposerState.SetOtherProposalID(oPaxosMsg.rejectbypromiseid());
    }
}

void Proposer :: OnPrepareTimeout()
{
    PLGHead("OK");

    if (GetInstanceID() != m_llTimeoutInstanceID)
    {
        PLGErr("TimeoutInstanceID %lu not same to NowInstanceID %lu, skip",
                m_llTimeoutInstanceID, GetInstanceID());
        return;
    }

    BP->GetProposerBP()->PrepareTimeout();

    // 如果是因为与其它节点发生冲突，重新分配 proposalID 值。
    Prepare(m_bWasRejectBySomeone);
}

void Proposer :: OnAcceptTimeout()
{
    PLGHead("OK");
    
    if (GetInstanceID() != m_llTimeoutInstanceID)
    {
        PLGErr("TimeoutInstanceID %lu not same to NowInstanceID %lu, skip",
                m_llTimeoutInstanceID, GetInstanceID());
        return;
    }
    
    BP->GetProposerBP()->AcceptTimeout();

    // 同上
    Prepare(m_bWasRejectBySomeone);
}

void Proposer :: CancelSkipPrepare()
{
    m_bCanSkipPrepare = false;
}

}


