/*
 *  Copyright (c) 2026, The OpenThread Authors.
 *  All rights reserved.
 *
 *  Redistribution and use in source and binary forms, with or without
 *  modification, are permitted provided that the following conditions are met:
 *  1. Redistributions of source code must retain the above copyright
 *     notice, this list of conditions and the following disclaimer.
 *  2. Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *  3. Neither the name of the copyright holder nor the
 *     names of its contributors may be used to endorse or promote products
 *     derived from this software without specific prior written permission.
 *
 *  THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 *  AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 *  IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 *  ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
 *  LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 *  CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 *  SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 *  INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 *  CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 *  ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 *  POSSIBILITY OF SUCH DAMAGE.
 */

#include <stdio.h>
#include <string.h>

#include "platform/nexus_core.hpp"
#include "platform/nexus_node.hpp"
#include "mac/mac.hpp"
#include "thread/child_table.hpp"
#include "thread/neighbor_table.hpp"
#include "thread/network_data_leader.hpp"
#include "thread/network_data_local.hpp"
#include "thread/network_data_notifier.hpp"

namespace ot {
namespace Nexus {

static constexpr uint16_t kMaxSeds    = 120;
static constexpr uint32_t kPollPeriod = 600;        // In milliseconds.
static constexpr uint32_t kWindow     = 150 * 1000; // Longer than `Mle::kMaxNeighborAge` (100 s).

struct Network
{
    explicit Network(uint16_t aNumSeds)
        : mLeader(mNexus.CreateNode())
        , mRouter(mNexus.CreateNode())
        , mNumSeds(aNumSeds)
    {
        mLeader.SetName("LEADER");
        mRouter.SetName("ROUTER");

        for (uint16_t i = 0; i < mNumSeds; i++)
        {
            mSeds[i] = &mNexus.CreateNode();
            mSeds[i]->SetName("SED", i);
        }

        mNexus.AdvanceTime(0);

        SuccessOrQuit(Instance::SetGlobalLogLevel(kLogLevelWarn));

        mLeader.Form();
        mNexus.AdvanceTime(15 * 1000);
        VerifyOrQuit(mLeader.Get<Mle::Mle>().IsLeader());

        mRouter.Join(mLeader);
        mNexus.AdvanceTime(200 * 1000);
        VerifyOrQuit(mRouter.Get<Mle::Mle>().IsRouter());

        for (uint16_t i = 0; i < mNumSeds; i++)
        {
            mSeds[i]->AllowList(mLeader);
            mSeds[i]->Join(mLeader, Node::kAsSed);
        }

        mNexus.AdvanceTime(120 * 1000);

        for (uint16_t i = 0; i < mNumSeds; i++)
        {
            VerifyOrQuit(mSeds[i]->Get<Mle::Mle>().IsChild());
            SuccessOrQuit(mSeds[i]->Get<DataPollSender>().SetExternalPollPeriod(kPollPeriod));
        }

        VerifyOrQuit(mLeader.Get<ChildTable>().GetNumChildren(Child::kInStateValid) == mNumSeds);

        mNexus.AdvanceTime(30 * 1000);
    }

    uint32_t GetLeaderTxBroadcasts(void) { return mLeader.Get<Mac::Mac>().GetCounters().mTxBroadcast; }
    uint32_t GetLeaderRxDataPolls(void) { return mLeader.Get<Mac::Mac>().GetCounters().mRxDataPoll; }

    uint16_t CountChildrenWithStaleNetworkData(void)
    {
        uint16_t count = 0;

        for (Child &child : mLeader.Get<ChildTable>().Iterate(Child::kInStateValid))
        {
            if (child.GetNetworkDataVersion() !=
                mLeader.Get<NetworkData::Leader>().GetVersion(child.GetNetworkDataType()))
            {
                count++;
            }
        }

        return count;
    }

    bool IsLeaderNeighborOfRouter(void)
    {
        return mRouter.Get<NeighborTable>().FindNeighbor(mLeader.Get<Mac::Mac>().GetExtAddress(),
                                                         Neighbor::kInStateValid) != nullptr;
    }

    void LogLinks(void)
    {
        Log("Router role:%s, leader is a valid neighbor of router:%s, leader children:%u",
            Mle::RoleToString(mRouter.Get<Mle::Mle>().GetRole()), ToYesNo(IsLeaderNeighborOfRouter()),
            mLeader.Get<ChildTable>().GetNumChildren(Child::kInStateValid));
    }

    void VerifyLeaderKeptItsLinks(void)
    {
        VerifyOrQuit(IsLeaderNeighborOfRouter());
        VerifyOrQuit(mRouter.Get<Mle::Mle>().IsRouter());
        VerifyOrQuit(mLeader.Get<ChildTable>().GetNumChildren(Child::kInStateValid) == mNumSeds);
    }

    Core     mNexus;
    Node    &mLeader;
    Node    &mRouter;
    Node    *mSeds[kMaxSeds];
    uint16_t mNumSeds;
};

void TestNetworkDataChangeWithSlowParentTx(void)
{
    // A network data change makes the leader queue a fragmented MLE
    // Child Update Request for every sleepy child. The leader takes
    // `kTxLatency` to put each frame on the air (a slow host to radio
    // path), so a child waiting behind others can time out and go back
    // to sleep before its frame arrives. The children must still get
    // the new network data, and the leader must keep sending its MLE
    // Advertisements so that the router keeps its link.

    static constexpr uint16_t kNumSeds     = 120;
    static constexpr uint16_t kNumPrefixes = 10;    // Makes the Child Update Request span three fragments.
    static constexpr uint32_t kTxLatency   = 8000;  // In microseconds.

    Network  network(kNumSeds);
    uint32_t broadcasts;
    uint32_t polls;
    uint16_t staleChildren;

    Log("---------------------------------------------------------------------------------------");
    Log("TestNetworkDataChangeWithSlowParentTx");

    network.mLeader.mRadio.mTxLatency = kTxLatency;

    for (uint16_t i = 0; i < kNumPrefixes; i++)
    {
        NetworkData::OnMeshPrefixConfig config;
        char                            string[20];

        config.Clear();
        snprintf(string, sizeof(string), "fd00:%x::/64", i + 1);
        SuccessOrQuit(config.GetPrefix().FromString(string));
        config.mOnMesh = true;
        config.mStable = true;
        SuccessOrQuit(network.mLeader.Get<NetworkData::Local>().AddOnMeshPrefix(config));
    }

    network.mLeader.Get<NetworkData::Notifier>().HandleServerDataUpdated();

    broadcasts = network.GetLeaderTxBroadcasts();
    polls      = network.GetLeaderRxDataPolls();

    network.mNexus.AdvanceTime(kWindow);

    broadcasts    = network.GetLeaderTxBroadcasts() - broadcasts;
    polls         = network.GetLeaderRxDataPolls() - polls;
    staleChildren = network.CountChildrenWithStaleNetworkData();

    Log("Leader tx broadcasts:%lu, rx data polls:%lu, children with stale network data:%u", ToUlong(broadcasts),
        ToUlong(polls), staleChildren);
    network.LogLinks();

    VerifyOrQuit(staleChildren == 0);
    VerifyOrQuit(broadcasts > 0);
    network.VerifyLeaderKeptItsLinks();
}

void TestIndirectBacklogForManySleepyChildren(void)
{
    // Every sleepy child of the leader always has more than one queued
    // message, so every indirect frame carries the frame-pending bit
    // and the children poll back to back. The leader must keep sending
    // its MLE Advertisements so that the router keeps its link.

    static constexpr uint16_t kNumSeds         = 20;
    static constexpr uint16_t kPayloadSize     = 250; // Three fragments per message.
    static constexpr uint32_t kTopUpInterval   = 10; // In milliseconds.
    static constexpr uint8_t  kMinQueuedPerSed = 2;

    Network  network(kNumSeds);
    uint32_t broadcasts;
    uint32_t polls;

    Log("---------------------------------------------------------------------------------------");
    Log("TestIndirectBacklogForManySleepyChildren");

    broadcasts = network.GetLeaderTxBroadcasts();
    polls      = network.GetLeaderRxDataPolls();

    for (uint32_t elapsed = 0; elapsed < kWindow; elapsed += kTopUpInterval)
    {
        for (uint16_t i = 0; i < kNumSeds; i++)
        {
            Node  &sed   = *network.mSeds[i];
            Child *child = network.mLeader.Get<ChildTable>().FindChild(sed.Get<Mac::Mac>().GetExtAddress(),
                                                                       Child::kInStateValid);

            if ((child != nullptr) && (child->GetIndirectMessageCount() < kMinQueuedPerSed))
            {
                network.mLeader.SendEchoRequest(sed.Get<Mle::Mle>().GetMeshLocalEid(), /* aIdentifier */ 0,
                                                kPayloadSize);
            }
        }

        network.mNexus.AdvanceTime(kTopUpInterval);
    }

    broadcasts = network.GetLeaderTxBroadcasts() - broadcasts;
    polls      = network.GetLeaderRxDataPolls() - polls;

    Log("Leader tx broadcasts:%lu, rx data polls:%lu", ToUlong(broadcasts), ToUlong(polls));
    network.LogLinks();

    VerifyOrQuit(broadcasts > 0);
    network.VerifyLeaderKeptItsLinks();
}

} // namespace Nexus
} // namespace ot

int main(int argc, char *argv[])
{
    // An optional argument selects a single test, since each one quits
    // on its first failed check.

    const char *name = (argc > 1) ? argv[1] : nullptr;

    if ((name == nullptr) || (strcmp(name, "backlog") == 0))
    {
        ot::Nexus::TestIndirectBacklogForManySleepyChildren();
    }

    if ((name == nullptr) || (strcmp(name, "netdata") == 0))
    {
        ot::Nexus::TestNetworkDataChangeWithSlowParentTx();
    }

    printf("All tests passed\n");
    return 0;
}
