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

#pragma once

#include "phxpaxos/node.h"
#include "kvsm.h"
#include <string>
#include <vector>
#include "phxpaxos_plugin/logger_google.h"
#include "log.h"
#include "def.h"

namespace phxkv
{


class PhxKV
{
public:
    PhxKV(const phxpaxos::NodeInfo & oMyNode, const phxpaxos::NodeInfoList & vecNodeList,
            const std::string & sKVDBPath, const std::string & sPaxosLogPath);
    ~PhxKV();
	
    // 主要的工作都在这里。
    int RunPaxos();

    // 获取集群的 master 节点信息，其实是个皮包函数，上家是 PNode 。
    const phxpaxos::NodeInfo GetMaster(const std::string & sKey);

    // 和上述函数一个性质，获知自己是否为 master 。
    const bool IsIMMaster(const std::string & sKey);

    PhxKVStatus Put(
            const std::string & sKey, 
            const std::string & sValue, 
            const uint64_t llVersion = NullVersion);

    PhxKVStatus GetLocal(
            const std::string & sKey, 
            std::string & sValue, 
            uint64_t & llVersion);

    PhxKVStatus Delete( 
            const std::string & sKey, 
            const uint64_t llVersion = NullVersion);

private:
    int GetGroupIdx(const std::string & sKey);

    // paxos 入口，是不是看到了熟悉的词?
    int KVPropose(const std::string & sKey, const std::string & sPaxosValue, PhxKVSMCtx & oPhxKVSMCtx);

private:
    phxpaxos::NodeInfo m_oMyNode;
    phxpaxos::NodeInfoList m_vecNodeList;
    std::string m_sKVDBPath;
    std::string m_sPaxosLogPath;

    // Group 数量，只是为了并发，和 paxos 本身无关。
    int m_iGroupCount;
    // 这是 phxpaxos 最核心的类，寄存着 node 的一切关于 paxos 的信息，
    // Node 是个抽象类，真正的类是 PNode 。
    phxpaxos::Node * m_poPaxosNode;

    // 本例的状态机定义。
    PhxKVSM m_oPhxKVSM;
};
    
}


