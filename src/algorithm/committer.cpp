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

#include "committer.h"
#include "commitctx.h"
#include "ioloop.h"
#include "commdef.h"

namespace phxpaxos
{

Committer :: Committer(Config * poConfig, CommitCtx * poCommitCtx, IOLoop * poIOLoop, SMFac * poSMFac)
    : m_poConfig(poConfig), m_poCommitCtx(poCommitCtx), m_poIOLoop(poIOLoop), m_poSMFac(poSMFac), m_iTimeoutMs(-1)
{
    m_llLastLogTime = Time::GetSteadyClockMS();
}

Committer :: ~Committer()
{
}

int Committer :: NewValue(const std::string & sValue)
{
    uint64_t llInstanceID = 0;
    return NewValueGetID(sValue, llInstanceID, nullptr);
}

int Committer :: NewValueGetID(const std::string & sValue, uint64_t & llInstanceID)
{
    return NewValueGetID(sValue, llInstanceID, nullptr);
}

int Committer :: NewValueGetID(const std::string & sValue, uint64_t & llInstanceID, SMCtx * poSMCtx)
{
    BP->GetCommiterBP()->NewValue();

    // 最多可以尝试三次，作者似乎认为这里没必要使用宏。
    int iRetryCount = 3;
    int ret = PaxosTryCommitRet_OK;
    while(iRetryCount--)
    {
        TimeStat oTimeStat;
        oTimeStat.Point();

        // 每次 step 的动作接口。
        ret = NewValueGetIDNoRetry(sValue, llInstanceID, poSMCtx);
        if (ret != PaxosTryCommitRet_Conflict)
        {
            if (ret == 0)
            {
                BP->GetCommiterBP()->NewValueCommitOK(oTimeStat.Point());
            }
            else
            {
                BP->GetCommiterBP()->NewValueCommitFail();
            }
            break;
        }

        BP->GetCommiterBP()->NewValueConflict();

        if (poSMCtx != nullptr && poSMCtx->m_iSMID == MASTER_V_SMID)
        {
            //master sm not retry
            break;
        }
    }

    return ret;
}

int Committer :: NewValueGetIDNoRetry(const std::string & sValue, uint64_t & llInstanceID, SMCtx * poSMCtx)
{
    LogStatus();

    int iLockUseTimeMs = 0;
    bool bHasLock = m_oWaitLock.Lock(m_iTimeoutMs, iLockUseTimeMs);
    if (!bHasLock)
    {
        // 两种情况会拿不到锁，一种是拿锁的过程超时了，
        // 还有一种是有太多的线程在等待锁。
        if (iLockUseTimeMs > 0)
        {
            BP->GetCommiterBP()->NewValueGetLockTimeout();
            PLGErr("Try get lock, but timeout, lockusetime %dms", iLockUseTimeMs);
            return PaxosTryCommitRet_Timeout; 
        }
        else
        {
            BP->GetCommiterBP()->NewValueGetLockReject();
            PLGErr("Try get lock, but too many thread waiting, reject");
            return PaxosTryCommitRet_TooManyThreadWaiting_Reject;
        }
    }

    int iLeftTimeoutMs = -1;
    if (m_iTimeoutMs > 0)
    {
        // 计算剩下还有多少时间可以运行本次 commit 。
        iLeftTimeoutMs = m_iTimeoutMs > iLockUseTimeMs ? m_iTimeoutMs - iLockUseTimeMs : 0;
        if (iLeftTimeoutMs < 200)
        {
            PLGErr("Get lock ok, but lockusetime %dms too long, lefttimeout %dms", iLockUseTimeMs, iLeftTimeoutMs);

            BP->GetCommiterBP()->NewValueGetLockTimeout();

            m_oWaitLock.UnLock();
            return PaxosTryCommitRet_Timeout;
        }
    }

    PLGImp("GetLock ok, use time %dms", iLockUseTimeMs);
    
    BP->GetCommiterBP()->NewValueGetLockOK(iLockUseTimeMs);

    //pack smid to value
    int iSMID = poSMCtx != nullptr ? poSMCtx->m_iSMID : 0;
    
    string sPackSMIDValue = sValue;
    // 消息需要合并一个 iSMID 作为状态机的辨识，方便以后状态机的执行。
    m_poSMFac->PackPaxosValue(sPackSMIDValue, iSMID);

    // 初始化 commit 类。 
    m_poCommitCtx->NewCommit(&sPackSMIDValue, poSMCtx, iLeftTimeoutMs);
    // 唤醒消费者。
    m_poIOLoop->AddNotify();

    // 等待最后的结果，等待过程中会休眠。
    int ret = m_poCommitCtx->GetResult(llInstanceID);

    m_oWaitLock.UnLock();
    return ret;
}

////////////////////////////////////////////////////

void Committer :: SetTimeoutMs(const int iTimeoutMs)
{
    m_iTimeoutMs = iTimeoutMs;
}

void Committer :: SetMaxHoldThreads(const int iMaxHoldThreads)
{
    m_oWaitLock.SetMaxWaitLogCount(iMaxHoldThreads);
}

void Committer :: SetProposeWaitTimeThresholdMS(const int iWaitTimeThresholdMS)
{
    m_oWaitLock.SetLockWaitTimeThreshold(iWaitTimeThresholdMS);
}

////////////////////////////////////////////////////

void Committer :: LogStatus()
{
    uint64_t llNowTime = Time::GetSteadyClockMS();
    // 超过 1s 才做一次 log ，防止写盘次数过多。
    if (llNowTime > m_llLastLogTime
            && llNowTime - m_llLastLogTime > 1000)
    {
        m_llLastLogTime = llNowTime;
        PLGStatus("wait threads %d avg thread wait ms %d reject rate %d",
                m_oWaitLock.GetNowHoldThreadCount(), m_oWaitLock.GetNowAvgThreadWaitTime(),
                m_oWaitLock.GetNowRejectRate());
    }
}
    
}


