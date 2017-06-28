/*
 * voting.h
 * Copyright (c) 2ndQuadrant, 2010-2017
 */
#ifndef _VOTING_H_
#define _VOTING_H_

typedef enum {
	VS_UNKNOWN = -1,
	VS_NO_VOTE,
	VS_VOTE_REQUEST_RECEIVED,
	VS_VOTE_INITIATED,
	VS_VOTE_WON,
	VS_VOTE_LOST
} NodeVotingStatus;

#endif /* _VOTING_H_ */
