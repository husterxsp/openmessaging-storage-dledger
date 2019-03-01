/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.openmessaging.storage.dledger;

import com.alibaba.fastjson.JSON;
import io.openmessaging.storage.dledger.protocol.*;
import io.openmessaging.storage.dledger.utils.DLedgerUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static io.openmessaging.storage.dledger.protocol.DLedgerResponseCode.EXPIRED_TERM;
import static io.openmessaging.storage.dledger.protocol.VoteResponse.RESULT.*;

public class DLedgerLeaderElector {

    private static Logger logger = LoggerFactory.getLogger(DLedgerLeaderElector.class);
    private final MemberState memberState;
    private Random random = new Random();
    private DLedgerConfig dLedgerConfig;
    private DLedgerRpcService dLedgerRpcService;

    //as a server handler
    //record the last leader state
    private long lastLeaderHeartBeatTime = -1;
    private long lastSendHeartBeatTime = -1;
    private long lastSuccHeartBeatTime = -1;
    private int heartBeatTimeIntervalMs = 2000;

    /**
     * ???
     * */
    private int maxHeartBeatLeak = 3;

    /**
     * as a client
     * 初始值为 -1。初始情况下，启动之后，就立即发起投票？不用等待随机时间了么。。。也行吧。。
     */
    private long nextTimeToRequestVote = -1;

    private boolean needIncreaseTermImmediately = false;

    private int minVoteIntervalMs = 300;
    private int maxVoteIntervalMs = 1000;

    /**
     * 这个数组目前没有用到，不知道啥用
     * */
    private List<RoleChangeHandler> roleChangeHandlers = new ArrayList<>();


    /**
     * 初始值是 WAIT_TO_REVOTE， 那么下面开始投票的时候就不会直接自增term了。
     * 注意不能是
     */
    private VoteResponse.ParseResult lastParseResult = VoteResponse.ParseResult.WAIT_TO_REVOTE;

    /**
     * 上一次投票耗时
     * */
    private long lastVoteCost = 0L;

    private StateMaintainer stateMaintainer = new StateMaintainer("StateMaintainer", logger);

    public DLedgerLeaderElector(DLedgerConfig dLedgerConfig, MemberState memberState, DLedgerRpcService dLedgerRpcService) {
        this.dLedgerConfig = dLedgerConfig;
        this.memberState = memberState;
        this.dLedgerRpcService = dLedgerRpcService;

        refreshIntervals(dLedgerConfig);
    }

    /**
     * 开始leader选举
     */
    public void startup() {
        /**
         * 启动一个状态机管理线程
         * */
        stateMaintainer.start();

        for (RoleChangeHandler roleChangeHandler : roleChangeHandlers) {
            roleChangeHandler.startup();
        }
    }

    public void shutdown() {
        stateMaintainer.shutdown();
        for (RoleChangeHandler roleChangeHandler : roleChangeHandlers) {
            roleChangeHandler.shutdown();
        }
    }

    private void refreshIntervals(DLedgerConfig dLedgerConfig) {
        this.heartBeatTimeIntervalMs = dLedgerConfig.getHeartBeatTimeIntervalMs();
        this.maxHeartBeatLeak = dLedgerConfig.getMaxHeartBeatLeak();
        this.minVoteIntervalMs = dLedgerConfig.getMinVoteIntervalMs();
        this.maxVoteIntervalMs = dLedgerConfig.getMaxVoteIntervalMs();
    }

    /**
     * 处理收到的心跳请求
     * @param request
     * @return
     * @throws Exception
     */
    public CompletableFuture<HeartBeatResponse> handleHeartBeat(HeartBeatRequest request) throws Exception {


        /**
         * 未知leaderId
         * */
        if (!memberState.isPeerMember(request.getLeaderId())) {
            logger.warn("[BUG] [HandleHeartBeat] remoteId={} is an unknown member", request.getLeaderId());
            return CompletableFuture.completedFuture(new HeartBeatResponse().term(memberState.currTerm()).code(DLedgerResponseCode.UNKNOWN_MEMBER.getCode()));
        }

        /**
         * leader就是自己
         * */
        if (memberState.getSelfId().equals(request.getLeaderId())) {
            logger.warn("[BUG] [HandleHeartBeat] selfId={} but remoteId={}", memberState.getSelfId(), request.getLeaderId());
            return CompletableFuture.completedFuture(new HeartBeatResponse().term(memberState.currTerm()).code(DLedgerResponseCode.UNEXPECTED_MEMBER.getCode()));
        }

        if (request.getTerm() < memberState.currTerm()) {
            /**
             * leaderID的term比当前term小
             * */
            return CompletableFuture.completedFuture(new HeartBeatResponse().term(memberState.currTerm()).code(EXPIRED_TERM.getCode()));
        } else if (request.getTerm() == memberState.currTerm()) {
            /**
             * 心跳请求的leader和当前节点认为的leader是同一个，更新心跳请求接收的时间
             * */
            if (request.getLeaderId().equals(memberState.getLeaderId())) {

                lastLeaderHeartBeatTime = System.currentTimeMillis();
                return CompletableFuture.completedFuture(new HeartBeatResponse());
            }
        }

        /**
         * 以下是非正常情况？
         * 啥意思？？？
         * */
        //abnormal case
        //hold the lock to get the latest term and leaderId
        synchronized (memberState) {
            /**
             * 这个判断上面不是已经有了吗？。。
             * */
            if (request.getTerm() < memberState.currTerm()) {
                return CompletableFuture.completedFuture(new HeartBeatResponse().term(memberState.currTerm()).code(EXPIRED_TERM.getCode()));
            } else if (request.getTerm() == memberState.currTerm()) {
                if (memberState.getLeaderId() == null) {
                    changeRoleToFollower(request.getTerm(), request.getLeaderId());
                    return CompletableFuture.completedFuture(new HeartBeatResponse());
                } else if (request.getLeaderId().equals(memberState.getLeaderId())) {
                    lastLeaderHeartBeatTime = System.currentTimeMillis();
                    return CompletableFuture.completedFuture(new HeartBeatResponse());
                } else {
                    //this should not happen, but if happened
                    logger.error("[{}][BUG] currTerm {} has leader {}, but received leader {}", memberState.getSelfId(), memberState.currTerm(), memberState.getLeaderId(), request.getLeaderId());
                    return CompletableFuture.completedFuture(new HeartBeatResponse().code(DLedgerResponseCode.INCONSISTENT_LEADER.getCode()));
                }
            } else {
                //To make it simple, for larger term, do not change to follower immediately
                //first change to candidate, and notify the state-maintainer thread
                changeRoleToCandidate(request.getTerm());
                needIncreaseTermImmediately = true;
                //TODO notify
                return CompletableFuture.completedFuture(new HeartBeatResponse().code(DLedgerResponseCode.TERM_NOT_READY.getCode()));
            }
        }
    }

    /**
     * 变成leader
     */
    public void changeRoleToLeader(long term) {
        synchronized (memberState) {

            // 这里需要再检查一下吗？？？
            // 都选举成功了，咋还需要检查term？
            if (memberState.currTerm() == term) {
                memberState.changeToLeader(term);
                lastSendHeartBeatTime = -1;

                handleRoleChange(term, MemberState.Role.LEADER);

                logger.info("[{}] [ChangeRoleToLeader] from term: {} and currTerm: {}", memberState.getSelfId(), term, memberState.currTerm());
            } else {
                logger.warn("[{}] skip to be the leader in term: {}, but currTerm is: {}", memberState.getSelfId(), term, memberState.currTerm());
            }
        }
    }


    /**
     * 变为候选者
     */
    public void changeRoleToCandidate(long term) {
        synchronized (memberState) {
            if (term >= memberState.currTerm()) {
                memberState.changeToCandidate(term);
                handleRoleChange(term, MemberState.Role.CANDIDATE);
                logger.info("[{}] [ChangeRoleToCandidate] from term: {} and currTerm: {}", memberState.getSelfId(), term, memberState.currTerm());
            } else {
                logger.info("[{}] skip to be candidate in term: {}, but currTerm: {}", memberState.getSelfId(), term, memberState.currTerm());
            }
        }
    }

    //just for test
    public void testRevote(long term) {
        changeRoleToCandidate(term);
        lastParseResult = VoteResponse.ParseResult.WAIT_TO_VOTE_NEXT;
        nextTimeToRequestVote = -1;
    }

    /**
     * 变成follower
     */
    public void changeRoleToFollower(long term, String leaderId) {
        logger.info("[{}][ChangeRoleToFollower] from term: {} leaderId: {} and currTerm: {}", memberState.getSelfId(), term, leaderId, memberState.currTerm());
        memberState.changeToFollower(term, leaderId);
        lastLeaderHeartBeatTime = System.currentTimeMillis();
        handleRoleChange(term, MemberState.Role.FOLLOWER);
    }

    public CompletableFuture<VoteResponse> handleVote(VoteRequest request, boolean self) {
        //hold the lock to get the latest term, leaderId, ledgerEndIndex
        synchronized (memberState) {

            /**
             * 收到未知节点的选举请求
             * */
            if (!memberState.isPeerMember(request.getLeaderId())) {
                logger.warn("[BUG] [HandleVote] remoteId={} is an unknown member", request.getLeaderId());
                return CompletableFuture.completedFuture(new VoteResponse(request).term(memberState.currTerm()).voteResult(REJECT_UNKNOWN_LEADER));
            }

            /**
             * self为false, 但实际上请求的leader却是自己。。。
             * */
            if (!self && memberState.getSelfId().equals(request.getLeaderId())) {
                logger.warn("[BUG] [HandleVote] selfId={} but remoteId={}", memberState.getSelfId(), request.getLeaderId());
                return CompletableFuture.completedFuture(new VoteResponse(request).term(memberState.currTerm()).voteResult(REJECT_UNEXPECTED_LEADER));
            }


            if (request.getTerm() < memberState.currTerm()) {
                /**
                 * 请求投票的term太低。拒绝
                 * */
                return CompletableFuture.completedFuture(new VoteResponse(request).term(memberState.currTerm()).voteResult(REJECT_EXPIRED_VOTE_TERM));

            } else if (request.getTerm() == memberState.currTerm()) {

                if (memberState.currVoteFor() == null) {
                    //let it go
                } else if (memberState.currVoteFor().equals(request.getLeaderId())) {
                    //repeat just let it go
                } else {
                    /**
                     * 当前节点认为有leader。拒绝
                     * 一种可能的情况：请求投票的节点与leader的连接断开，但是当前这个节点还能连接到leader
                     */
                    if (memberState.getLeaderId() != null) {
                        return CompletableFuture.completedFuture(new VoteResponse(request).term(memberState.currTerm()).voteResult(REJECT_ALREADY__HAS_LEADER));
                    } else {
                        /**
                         * 已经投过票了
                         * */
                        return CompletableFuture.completedFuture(new VoteResponse(request).term(memberState.currTerm()).voteResult(REJECT_ALREADY_VOTED));
                    }
                }
            } else {
                /**
                 * 请求投票的term比当前大，为啥 REJECT_TERM_NOT_READY ？？？
                 * */
                //stepped down by larger term
                changeRoleToCandidate(request.getTerm());
                needIncreaseTermImmediately = true;

                /**
                 * 为啥只有term一致的时候才能投票。。。？？？
                 * */
                // only can handleVote when the term is consistent
                return CompletableFuture.completedFuture(new VoteResponse(request).term(memberState.currTerm()).voteResult(REJECT_TERM_NOT_READY));
            }

            /**
             * getLedgerEndTerm / getLedgerEndIndex 怎么理解
             * */
            //assert acceptedTerm is true
            if (request.getLedgerEndTerm() < memberState.getLedgerEndTerm()) {
                return CompletableFuture.completedFuture(new VoteResponse(request).term(memberState.currTerm()).voteResult(REJECT_EXPIRED_LEDGER_TERM));
            } else if (request.getLedgerEndTerm() == memberState.getLedgerEndTerm() && request.getLedgerEndIndex() < memberState.getLedgerEndIndex()) {
                return CompletableFuture.completedFuture(new VoteResponse(request).term(memberState.currTerm()).voteResult(REJECT_SMALL_LEDGER_END_INDEX));
            }

            if (request.getTerm() < memberState.getLedgerEndTerm()) {
                return CompletableFuture.completedFuture(new VoteResponse(request).term(memberState.getLedgerEndTerm()).voteResult(REJECT_TERM_SMALL_THAN_LEDGER));
            }

            /**
             * 确认投票
             * */
            memberState.setCurrVoteFor(request.getLeaderId());
            return CompletableFuture.completedFuture(new VoteResponse(request).term(memberState.currTerm()).voteResult(ACCEPT));

        }
    }


    /**
     *
     * leader 发送心跳
     *
     * @param term
     * @param leaderId
     * @throws Exception
     */
    private void sendHeartbeats(long term, String leaderId) throws Exception {

        final AtomicInteger allNum = new AtomicInteger(1);
        final AtomicInteger succNum = new AtomicInteger(1);
        final AtomicInteger notReadyNum = new AtomicInteger(0);
        final AtomicLong maxTerm = new AtomicLong(-1);
        final AtomicBoolean inconsistLeader = new AtomicBoolean(false);

        /**
         * 这个 new CountDownLatch(1); 的作用在下面的代码中就是等待一个异步请求执行完
         * */
        final CountDownLatch beatLatch = new CountDownLatch(1);
        long startHeartbeatTimeMs = System.currentTimeMillis();

        for (String id : memberState.getPeerMap().keySet()) {
            if (memberState.getSelfId().equals(id)) {
                continue;
            }

            HeartBeatRequest heartBeatRequest = new HeartBeatRequest();
            heartBeatRequest.setGroup(memberState.getGroup());
            heartBeatRequest.setLocalId(memberState.getSelfId());
            heartBeatRequest.setRemoteId(id);
            heartBeatRequest.setLeaderId(leaderId);
            heartBeatRequest.setTerm(term);

            CompletableFuture<HeartBeatResponse> future = dLedgerRpcService.heartBeat(heartBeatRequest);
            future.whenComplete((HeartBeatResponse x, Throwable ex) -> {
                try {

                    if (ex != null) {
                        throw ex;
                    }
                    switch (DLedgerResponseCode.valueOf(x.getCode())) {
                        case SUCCESS:
                            succNum.incrementAndGet();
                            break;
                        case EXPIRED_TERM:
                            /**
                             * 这里是不是不对？？
                             * 多个请求不应该比较一下吗？？
                             * */
                            maxTerm.set(x.getTerm());
                            break;
                        case INCONSISTENT_LEADER:
                            /**
                             * 待看
                             * */
                            inconsistLeader.compareAndSet(false, true);
                            break;
                        case TERM_NOT_READY:
                            notReadyNum.incrementAndGet();
                            break;
                        default:
                            break;
                    }
                    if (memberState.isQuorum(succNum.get())
                            || memberState.isQuorum(succNum.get() + notReadyNum.get())) {
                        beatLatch.countDown();
                    }
                } catch (Throwable t) {
                    logger.error("Parse heartbeat response failed", t);
                } finally {
                    allNum.incrementAndGet();
                    if (allNum.get() == memberState.peerSize()) {
                        beatLatch.countDown();
                    }
                }
            });
        }

        beatLatch.await(heartBeatTimeIntervalMs, TimeUnit.MILLISECONDS);

        if (memberState.isQuorum(succNum.get())) {
            /**
             * 作为leader没啥问题，大部分节点成功回应hearbeat
             * */
            lastSuccHeartBeatTime = System.currentTimeMillis();
        } else {
            logger.info("[{}] Parse heartbeat responses in cost={} term={} allNum={} succNum={} notReadyNum={} inconsistLeader={} maxTerm={} peerSize={} lastSuccHeartBeatTime={}",
                    memberState.getSelfId(), DLedgerUtils.elapsed(startHeartbeatTimeMs), term, allNum.get(), succNum.get(), notReadyNum.get(), inconsistLeader.get(), maxTerm.get(), memberState.peerSize(), new Timestamp(lastSuccHeartBeatTime));
            if (memberState.isQuorum(succNum.get() + notReadyNum.get())) {
                /**
                 * 一部分节点的term落后。
                 * */
                lastSendHeartBeatTime = -1;
            } else if (maxTerm.get() > term) {
                /**
                 * 这里有个疑问：有没有可能，上面的 isQuorum(succNum.get()) 满足，这里的maxTerm.get() > term也满足？
                 * 有term比当前节点大。。。
                 * */
                changeRoleToCandidate(maxTerm.get());
            } else if (inconsistLeader.get()) {
                changeRoleToCandidate(term);
            } else if (DLedgerUtils.elapsed(lastSuccHeartBeatTime) > maxHeartBeatLeak * heartBeatTimeIntervalMs) {
                changeRoleToCandidate(term);
            }
        }


    }

    /**
     * 选举成功。
     */
    private void maintainAsLeader() throws Exception {
        if (DLedgerUtils.elapsed(lastSendHeartBeatTime) > heartBeatTimeIntervalMs) {
            long term;
            String leaderId;
            synchronized (memberState) {
                if (!memberState.isLeader()) {
                    //stop sending
                    return;
                }
                term = memberState.currTerm();
                leaderId = memberState.getLeaderId();
                lastSendHeartBeatTime = System.currentTimeMillis();
            }
            sendHeartbeats(term, leaderId);
        }
    }

    private void maintainAsFollower() {
        if (DLedgerUtils.elapsed(lastLeaderHeartBeatTime) > 2 * heartBeatTimeIntervalMs) {
            synchronized (memberState) {
                if (memberState.isFollower() && (DLedgerUtils.elapsed(lastLeaderHeartBeatTime) > maxHeartBeatLeak * heartBeatTimeIntervalMs)) {
                    logger.info("[{}][HeartBeatTimeOut] lastLeaderHeartBeatTime: {} heartBeatTimeIntervalMs: {} lastLeader={}", memberState.getSelfId(), new Timestamp(lastLeaderHeartBeatTime), heartBeatTimeIntervalMs, memberState.getLeaderId());
                    changeRoleToCandidate(memberState.currTerm());
                }
            }
        }
    }

    /**
     * 发起投票
     * @param term
     * @param ledgerEndTerm
     * @param ledgerEndIndex
     * @return
     * @throws Exception
     */
    private List<CompletableFuture<VoteResponse>> voteForQuorumResponses(long term, long ledgerEndTerm,
                                                                         long ledgerEndIndex) throws Exception {
        List<CompletableFuture<VoteResponse>> responses = new ArrayList<>();

        for (String id : memberState.getPeerMap().keySet()) {
            VoteRequest voteRequest = new VoteRequest();
            voteRequest.setGroup(memberState.getGroup());
            voteRequest.setLedgerEndIndex(ledgerEndIndex);
            voteRequest.setLedgerEndTerm(ledgerEndTerm);
            voteRequest.setLeaderId(memberState.getSelfId());
            voteRequest.setTerm(term);
            voteRequest.setRemoteId(id);
            CompletableFuture<VoteResponse> voteResponse;

            if (memberState.getSelfId().equals(id)) {

                // 给自己投票
                voteResponse = handleVote(voteRequest, true);

            } else {
                // async
                // 向其他节点发起投票
                voteResponse = dLedgerRpcService.vote(voteRequest);

            }

            responses.add(voteResponse);

        }
        return responses;
    }

    private long getNextTimeToRequestVote() {
        return System.currentTimeMillis() + lastVoteCost + minVoteIntervalMs + random.nextInt(maxVoteIntervalMs - minVoteIntervalMs);
    }

    /**
     * 保持作为候选者，这里会发起投票
     *
     * @throws Exception
     */
    private void maintainAsCandidate() throws Exception {
        // for candidate
        /**
         * 还没有超时，不需要投票
         */
        if (System.currentTimeMillis() < nextTimeToRequestVote && !needIncreaseTermImmediately) {
            return;
        }

        long term;
        long ledgerEndTerm;
        long ledgerEndIndex;


        /**
         * 投票开始前，根据之前的投票结果来判断是否需要增加term  (INCREASE_TERM)
         * 比如如果是prevote的情况，就不增加了。
         * */
        synchronized (memberState) {

            // 这里不知道有什么用？？
            // 准备投票的时候，状态可能会变化？

            if (!memberState.isCandidate()) {
                return;
            }

            /**
             * VoteResponse.ParseResult里的一些枚举变量。。没太搞明白。
             * */
            if (lastParseResult == VoteResponse.ParseResult.WAIT_TO_VOTE_NEXT || needIncreaseTermImmediately) {

                long prevTerm = memberState.currTerm();

                /**
                 * Provote 的问题
                 * https://www.jianshu.com/p/1496228df9a9
                 * term不是直接自增的吗?
                 * 这个函数nextTerm里又检查了一下有没有比自己更高的term。。。
                 * */
                term = memberState.nextTerm();

                logger.info("{} {} {}", lastParseResult, VoteResponse.ParseResult.WAIT_TO_VOTE_NEXT, needIncreaseTermImmediately);

                // 只有这里会
                logger.info("{}_[INCREASE_TERM] from {} to {}", memberState.getSelfId(), prevTerm, term);

                lastParseResult = VoteResponse.ParseResult.WAIT_TO_REVOTE;
            } else {

                term = memberState.currTerm();

            }

            // ledgerEndIndex， ledgerEndTerm 啥含义。。。
            ledgerEndIndex = memberState.getLedgerEndIndex();
            ledgerEndTerm = memberState.getLedgerEndTerm();
        }

        if (needIncreaseTermImmediately) {
            nextTimeToRequestVote = getNextTimeToRequestVote();
            needIncreaseTermImmediately = false;
            return;
        }

        long startVoteTimeMs = System.currentTimeMillis();

        /**
         * 投票结果收集
         * */
        final List<CompletableFuture<VoteResponse>> quorumVoteResponses = voteForQuorumResponses(term, ledgerEndTerm, ledgerEndIndex);

        final AtomicLong knownMaxTermInGroup = new AtomicLong(-1);
        final AtomicInteger allNum = new AtomicInteger(0);
        final AtomicInteger validNum = new AtomicInteger(0);
        final AtomicInteger acceptedNum = new AtomicInteger(0);
        final AtomicInteger notReadyTermNum = new AtomicInteger(0);
        final AtomicInteger biggerLedgerNum = new AtomicInteger(0);
        final AtomicBoolean alreadyHasLeader = new AtomicBoolean(false);

        CountDownLatch voteLatch = new CountDownLatch(1);

        for (CompletableFuture<VoteResponse> future : quorumVoteResponses) {

            logger.info("quorumVoteResponses size {}", quorumVoteResponses.size());

            future.whenComplete((VoteResponse x, Throwable ex) -> {
                try {
                    if (ex != null) {
                        throw ex;
                    }

                    logger.info("[{}][GetVoteResponse] {}", memberState.getSelfId(), JSON.toJSONString(x));

                    /**
                     * 一个合法请求
                     * */
                    if (x.getVoteResult() != VoteResponse.RESULT.UNKNOWN) {
                        validNum.incrementAndGet();
                    }

                    synchronized (knownMaxTermInGroup) {
                        switch (x.getVoteResult()) {
                            case ACCEPT:
                                // 确认投票
                                acceptedNum.incrementAndGet();
                                break;
                            case REJECT_ALREADY_VOTED:
                                // 拒绝，已经投过了
                                break;
                            case REJECT_ALREADY__HAS_LEADER:
                                // 拒绝，已经有leader了
                                alreadyHasLeader.compareAndSet(false, true);
                                break;
                            case REJECT_TERM_SMALL_THAN_LEDGER:
                            case REJECT_EXPIRED_VOTE_TERM:
                                // 请求接收方term比较大，这里更新一下本地
                                if (x.getTerm() > knownMaxTermInGroup.get()) {
                                    knownMaxTermInGroup.set(x.getTerm());
                                }
                                break;
                            case REJECT_EXPIRED_LEDGER_TERM:
                            case REJECT_SMALL_LEDGER_END_INDEX:
                                biggerLedgerNum.incrementAndGet();
                                break;
                            case REJECT_TERM_NOT_READY:
                                notReadyTermNum.incrementAndGet();
                                break;
                            default:
                                break;

                        }
                    }
                    /**
                     * 1. 已经有leader
                     * 2. 选举成功
                     * 3. 还是上面说的问题，为啥只有term一致的情况下才发起投票
                     * */
                    if (alreadyHasLeader.get()
                            || memberState.isQuorum(acceptedNum.get())
                            || memberState.isQuorum(acceptedNum.get() + notReadyTermNum.get())) {
                        voteLatch.countDown();
                    }
                } catch (Throwable t) {
                    logger.error("Get error when parsing vote response ", t);
                } finally {
                    allNum.incrementAndGet();
                    if (allNum.get() == memberState.peerSize()) {
                        voteLatch.countDown();
                    }
                }
            });

        }

        // 投票阶段结束，等待 收到所有的投票结果。
        try {
            voteLatch.await(3000 + random.nextInt(maxVoteIntervalMs), TimeUnit.MILLISECONDS);
        } catch (Throwable ignore) {

        }

        lastVoteCost = DLedgerUtils.elapsed(startVoteTimeMs);

        VoteResponse.ParseResult parseResult;

        if (knownMaxTermInGroup.get() > term) {

            /**
             * 当前term比较小
             * WAIT_TO_VOTE_NEXT, 下次循环会自增term
             * */
            parseResult = VoteResponse.ParseResult.WAIT_TO_VOTE_NEXT;
            nextTimeToRequestVote = getNextTimeToRequestVote();

            // 更新term, 继续发起选举
            changeRoleToCandidate(knownMaxTermInGroup.get());

        } else if (alreadyHasLeader.get()) {

            parseResult = VoteResponse.ParseResult.WAIT_TO_VOTE_NEXT;
            nextTimeToRequestVote = getNextTimeToRequestVote() + heartBeatTimeIntervalMs * maxHeartBeatLeak;

        } else if (!memberState.isQuorum(validNum.get())) {

            /**
             * prevote
             * WAIT_TO_REVOTE 下次循环不用自增term
             * */
            // 请求成功数太少，重新选举
            parseResult = VoteResponse.ParseResult.WAIT_TO_REVOTE;
            nextTimeToRequestVote = getNextTimeToRequestVote();

        } else if (memberState.isQuorum(acceptedNum.get())) {

            // 选举成功
            parseResult = VoteResponse.ParseResult.PASSED;

            logger.info(String.valueOf(parseResult));

        } else if (memberState.isQuorum(acceptedNum.get() + notReadyTermNum.get())) {

            // 立即重新选举？
            parseResult = VoteResponse.ParseResult.REVOTE_IMMEDIATELY;

        } else if (memberState.isQuorum(acceptedNum.get() + biggerLedgerNum.get())) {

            parseResult = VoteResponse.ParseResult.WAIT_TO_REVOTE;
            nextTimeToRequestVote = getNextTimeToRequestVote();

        } else {

            parseResult = VoteResponse.ParseResult.WAIT_TO_VOTE_NEXT;
            nextTimeToRequestVote = getNextTimeToRequestVote();

        }

        lastParseResult = parseResult;

        logger.info("[{}] [PARSE_VOTE_RESULT] cost={} term={} memberNum={} allNum={} acceptedNum={} notReadyTermNum={} biggerLedgerNum={} alreadyHasLeader={} maxTerm={} result={}",
                memberState.getSelfId(), lastVoteCost, term, memberState.peerSize(), allNum, acceptedNum, notReadyTermNum, biggerLedgerNum, alreadyHasLeader, knownMaxTermInGroup.get(), parseResult);

        if (parseResult == VoteResponse.ParseResult.PASSED) {
            logger.info("[{}] [VOTE_RESULT] has been elected to be the leader in term {}", memberState.getSelfId(), term);
            // 选举成功，切换到leader
            changeRoleToLeader(term);
        }

    }

    /**
     * The core method of maintainer.
     * Run the specified logic according to the current role:
     * candidate => propose a vote.
     * leader => send heartbeats to followers, and step down to candidate when quorum followers do not respond.
     * follower => accept heartbeats, and change to candidate when no heartbeat from leader.
     *
     * @throws Exception
     */
    private void maintainState() throws Exception {
        if (memberState.isLeader()) {
            maintainAsLeader();
        } else if (memberState.isFollower()) {
            maintainAsFollower();
        } else {
            maintainAsCandidate();
        }
    }

    private void handleRoleChange(long term, MemberState.Role role) {
        for (RoleChangeHandler roleChangeHandler : roleChangeHandlers) {
            try {
                roleChangeHandler.handle(term, role);
            } catch (Throwable t) {
                logger.warn("Handle role change failed term={} role={} handler={}", term, role, roleChangeHandler.getClass(), t);
            }
        }
    }


    public void addRoleChangeHandler(RoleChangeHandler roleChangeHandler) {
        if (!roleChangeHandlers.contains(roleChangeHandler)) {
            roleChangeHandlers.add(roleChangeHandler);
        }
    }

    public interface RoleChangeHandler {
        void handle(long term, MemberState.Role role);

        void startup();

        void shutdown();
    }

    public class StateMaintainer extends ShutdownAbleThread {

        public StateMaintainer(String name, Logger logger) {
            super(name, logger);
        }

        @Override
        public void doWork() {
            try {
                /**
                 * 通过这种方式访问外部类对象？。。。
                 * */
                if (DLedgerLeaderElector.this.dLedgerConfig.isEnableLeaderElector()) {

                    // 这里为啥要再调用一次？
                    DLedgerLeaderElector.this.refreshIntervals(dLedgerConfig);
                    // 线程启动后，maintainState方法里面 会启动leader的选举
                    DLedgerLeaderElector.this.maintainState();
                }
                sleep(10);
            } catch (Throwable t) {
                DLedgerLeaderElector.logger.error("Error in heartbeat", t);
            }
        }

    }

}
