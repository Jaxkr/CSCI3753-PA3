/*
 * File: multi-lookup.c
 * Author: Jake Traut
 * Creator: Andy Sayler
 * Project: CSCI 3753 Programming Assignment 3
 * Create Date: 2012/02/01
 * Modify Date: 2016/03/13
 * Description:
 * 	This file contains the multi-threaded
 *      solution to this assignment, that resolves 
		domain names to IP addresses. 
 *  
 */

#include "multi-lookup.h"

#define MINARGS 3
#define USAGE "<inputFilePath> <outputFilePath>"
#define SBUFSIZE 1025 //equivalent to MAX_NAME_LENGTH 
#define INPUTFS "%1024s"
#define MAX_INPUT_FILES 10
#define MAX_RESOLVER_THREADS 10
#define MIN_RESOLVER_THREADS 2
#define MAX_NAME_LENGTH 1025
#define MAX_IP_LENGTH INET6_ADDRSTRLEN
#define QUEUE_SIZE 1024

queue q; //FIFO queue, not thread-safe, must protect from deadlock
int FILES_TO_SERVICE;
int files_serviced_by_producer = 0;
int files_serviced_by_consumer = 0;
char* OUTPUT_FILE; 

pthread_mutex_t queue_mutex;
pthread_mutex_t output_mutex;

void* producer(char* namefile){
	char hostname[SBUFSIZE]; //for individual hostname
	char errorstr[SBUFSIZE];	
	char* hostnamep; //pointer passed to queue
    int push_counter = 0;
    
   	if(files_serviced_by_producer == FILES_TO_SERVICE) {
		printf("All files have been serviced by producer");
		return NULL;
	}
	
	FILE* inputfp = fopen(namefile, "r");
	if(!inputfp){
	    sprintf(errorstr, "Error Opening Input File: %s", namefile);
	    printf("%s\n",errorstr);
	    return NULL;
	}	
	
	//read each file and push domain name onto FIFO queue
	while(fscanf(inputfp, INPUTFS, hostname) > 0){
		
		pthread_mutex_lock(&queue_mutex); //lock the queue, avoid deadlock
		//check if thread is full, if so sleep
		if(queue_is_full(&q)){
			pthread_mutex_unlock(&queue_mutex); //unlock to make room
			usleep(rand()%100); //sleep random period of time between 0-100 microseconds
			pthread_mutex_lock(&queue_mutex); //relock to push to queue
		}
		if(hostname == NULL){
			fprintf(stderr, "ERROR: hostname is null, not pushing to queue");
		}
		else{
			//malloc memory for pointer
			hostnamep = (char*) malloc(MAX_NAME_LENGTH*sizeof(char));
			strncpy(hostnamep, hostname, MAX_NAME_LENGTH);
			//push onto queue, and check for success
			if(queue_push(&q, hostnamep) == QUEUE_FAILURE){
				fprintf(stderr, "ERROR: queue_push failed for %s\n", hostname);
				push_counter--;
			}
			push_counter++; //increment counter for requester thread
		}
		pthread_mutex_unlock(&queue_mutex); //done with queue so unlock
	}
	
	printf("Requester thread added %d hostnames to queue.\n", push_counter);
	
	files_serviced_by_producer++;
	
	//done reading, close file and exit
	fclose(inputfp);
	pthread_exit(NULL);
	return NULL;
}

void* consumer(){
	FILE* outputfp = fopen(OUTPUT_FILE, "w");
	if(!outputfp){
		perror("Error Opening Output File");
		fprintf(stderr, "Error Opening Output File");
		return NULL;
    }
    
    char* hostname;
    char firstipstr[INET6_ADDRSTRLEN];
    int pop_counter = 0;
    int queue_status;
    int more_work = 0;
    
    //working with queue, avoid deadlock
    pthread_mutex_lock(&queue_mutex);
    queue_status = !queue_is_empty(&q); //check if empty or full
    pthread_mutex_unlock(&queue_mutex);    
    
    if(files_serviced_by_consumer < FILES_TO_SERVICE || queue_status){
		more_work = 1;
	}
		
    //Continue to pull from queue 
    while(more_work){
		pthread_mutex_lock(&queue_mutex);
		//pop hostname off queue and check for success
		if((hostname = queue_pop(&q)) == NULL){
			fprintf(stderr, "ERROR: queue_pop failed, hostname is null\n");
			pthread_mutex_unlock(&queue_mutex);
		}
		else{ //successful
			pthread_mutex_unlock(&queue_mutex);
			pop_counter++; //increment the counter for resolver thread
			
			 /* Lookup hostname and get IP string */
			if(dnslookup(hostname, firstipstr, sizeof(firstipstr)) == UTIL_FAILURE){
				fprintf(stderr, "dnslookup error: %s\n", hostname);
				strncpy(firstipstr, "", sizeof(firstipstr));
			}
			
			/* Write to Output File */
			pthread_mutex_lock(&output_mutex);
			fprintf(outputfp, "%s,%s\n", hostname, firstipstr);
			pthread_mutex_unlock(&output_mutex);
	
			free(hostname); //free memory after popping from queue
		}
		
		//check if work is done
		    //working with queue, avoid deadlock
		pthread_mutex_lock(&queue_mutex);
		queue_status = !queue_is_empty(&q); //check if empty 
		pthread_mutex_unlock(&queue_mutex);    

		if(files_serviced_by_consumer++ < FILES_TO_SERVICE || queue_status){
			more_work = 1;
		}
		else more_work = 0;
	}
	printf("Resolver thread processed %d hostnames from queue.\n", pop_counter); //print how much work thread did 
	pthread_exit(NULL);
	fclose(outputfp);
	return NULL;
}

int main(int argc, char* argv[]){

	FILES_TO_SERVICE = argc - 2; //-1 for original call -1 for resolve.txt
	char* name_files[FILES_TO_SERVICE];
	OUTPUT_FILE = argv[argc-1]; //last arg is the output file
	int i = 0;

	fflush(stdout);
	
	//Initialize the vars
	queue_init(&q, QUEUE_SIZE);
	pthread_mutex_init(&queue_mutex, NULL);
	pthread_mutex_init(&output_mutex, NULL);
	
    /* Check Arguments */
    if(argc < MINARGS){
	fprintf(stderr, "Not enough arguments: %d\n", (argc - 1));
	fprintf(stderr, "Usage:\n %s %s\n", argv[0], USAGE);
	return EXIT_FAILURE;
    }
	
	pthread_t requester_threads[FILES_TO_SERVICE];
	pthread_t resolver_threads[MAX_RESOLVER_THREADS];
	
	//loop through all input files 
	for(i=0; i < FILES_TO_SERVICE; i++){
		//creating threads for producer stage 
		pthread_create(&requester_threads[i], NULL, (void*) producer, (void*) argv[i+1]);
	}
	
	//first fill the queue
	for(i=0; i<FILES_TO_SERVICE; i++){
		pthread_join(requester_threads[i], NULL);
	}
	
	//create threads for the consumer stage
	for(i=0; i < MAX_RESOLVER_THREADS; i++){
		pthread_create(&resolver_threads[i], NULL, consumer, NULL);
	}
	//then resolve dns
	for(i=0; i<MAX_RESOLVER_THREADS; i++){
		pthread_join(resolver_threads[i], NULL);
	}
	
	queue_cleanup(&q); 
	pthread_mutex_destroy(&queue_mutex);
	pthread_mutex_destroy(&output_mutex);

    return EXIT_SUCCESS;
}
