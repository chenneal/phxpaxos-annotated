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

#include "phxpaxos/node.h"
#include "pnode.h"

namespace phxpaxos
{

int Node :: RunNode(const Options & oOptions, Node *& poNode)
{
    // 对大数据量的值做了专门的处理优化。
    if (oOptions.bIsLargeValueMode)
    {
        InsideOptions::Instance()->SetAsLargeBufferMode();
    }
    // 设置 group 的数量
    InsideOptions::Instance()->SetGroupCount(oOptions.iGroupCount);
        
    poNode = nullptr;
    NetWork * poNetWork = nullptr;

    // 可以经常看到这个 BP ，这里的 Breakpoint 其实是个单例，
    // 目的是为了方便调试。
    Breakpoint::m_poBreakpoint = nullptr;
    BP->SetInstance(oOptions.poBreakpoint);

    PNode * poRealNode = new PNode();
	
    // 很多结构的初始化工作都是在这个函数里面完成的。
    int ret = poRealNode->Init(oOptions, poNetWork);
    if (ret != 0)
    {
        delete poRealNode;
        return ret;
    }

    // 网络结构体指向上面刚刚new的 PNode 对象，以便正确回调。
    // 初始化工作实在上面的 Init 函数里完成的。
    //step1 set node to network
    //very important, let network on recieve callback can work.
    poNetWork->m_poNode = poRealNode;

    // 启动网络服务，这样 phxpaxos 在做算法的各种 rpc 通信时就通过
    // 这个网络服务传递消息与 log 。
    //step2 run network.
    //start recieve message from network, so all must init before this step.
    //must be the last step.
    poNetWork->RunNetWork();

    // 赋值给指针形参，以便外部能够正确访问。
    poNode = poRealNode;

    return 0;
}
    
}


