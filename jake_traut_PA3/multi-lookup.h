#ifndef MULTI_LOOKUP_H
#define MULTI_LOOKUP_H

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <pthread.h>
#include <unistd.h>
#include "util.h"
#include "queue.h"

/* Implementing the Producer-Consumer architecture. 
 * Producer sub-system uses requester thread pool
 * Consumer sub-system uses resolver thread pool */
 
 /* Services a set of name files, each hostname read is placed
  * on a FIFO queue. Runs with requester threads. */
void* producer(char* namefile);

/* Services the FIFO queue by taking a name off the queue and
 * querying its IP address. Runs with resolver threads. */
void* consumer();

/*Initialize local vars and run the thread architecture */
int main(int argc, char* argv[]);

#endif
