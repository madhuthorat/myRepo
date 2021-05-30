/**********************************************************************
 * This code implements a mini server which keeps on looking for new
 * requests to handle. It can handle 4 type of requests for a file:
 * 1. Create a file
 * 2. Read from a file
 * 3. Write to a file
 * 4. Delete a file
 *
 * It also implements a mini cache, which caches file name, FD related
 * information and lookups are made to the cache as needed.
 ***********************************************************************/

#include <stdio.h>
#include <sys/socket.h>
#include <string.h>
#include <pthread.h>

#define FILE_NAME_SIZE 100
#define MAX_CACHE_ENTRIES 1024
#define MAX_RW_SIZE 1024*1024
#define MAX_THREADS 256 
#define MAX_QUEUE_SIZE 256 

enum reqtype {
    CREATE=0,
    READ,
    WRITE,
    DELETE
};

typedef enum status {
    SUCCESS=0,
    FAILURE
}status_t;

typedef struct filename {
    char fname[FILE_NAME_SIZE];
}filename_t;

typedef struct cache {
    filename_t fn;
    FILE* fd;
    char openmode[4];
    pthread_rwlock_t rwlock;
}cache_t;

typedef struct createfile {
    char openmode[4];
}createfile_t;

typedef struct writefile {
    int pos;
    int whence; // can be SEEK_SET, SEEK_END, or SEEK_CUR
    int reqWSize; // requested write size
    int resWSize; // actual write size set as response write size
    char *writeBuf;
}writefile_t;

typedef struct readfile {
    int pos;
    int whence; // can be SEEK_SET, SEEK_END, or SEEK_CUR
    int reqRSize;
    char *readBuf;
}readfile_t;

typedef struct deletefile {
    // the file name for deletion is passed with "struct req"
    // this structure is placeholder for delete file request
    // no arguments added as of now
}deletefile_t;

typedef struct req {
    enum reqtype type;
    int len;
    filename_t fn;
    union requ {
        createfile_t cr;
        writefile_t wr;
        readfile_t rd;
        deletefile_t df;
    }u;
}req_t;

// Cache related functions
status_t initCache();
status_t deinitializeCache();
status_t cacheFd(filename_t *fn, FILE* fd, char *openmode);
status_t rmCacheFd(cache_t *entry);
cache_t* findFd(filename_t *fn);

// Other request type specific functions
status_t fCreate(req_t *request);
status_t fWrite(req_t *request);
status_t fRead(req_t *request);
status_t fDelete(req_t *request);

// Cache saving information about open FDs
cache_t fdmap[MAX_CACHE_ENTRIES];
int fdcnt=0; // count of current open FDs

// Mutex for cache code, create request code and queue handling code
pthread_mutex_t cacheMutex = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t createMutex = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t queueMutex = PTHREAD_MUTEX_INITIALIZER;

// Thread related function
void* workerThread(void *arg);
// The lookForWork tells a thread if should look for further work or not
int lookForWork=0; 

// Variables for request queue handling
// This can be improved further by having linked list for queue
req_t *reqQueue[MAX_QUEUE_SIZE];
int qReadPtr=0, qWritePtr=0, queuedRequests=0;

// This function initializes the cache entries
status_t initCache() {
	int i;
 
	printf("\n Initializing Cache\n");
	for(i=0;i<MAX_CACHE_ENTRIES;i++) {
		fdmap[i].fd = NULL; // Setting fd to NULL means entry is not in use
		pthread_rwlock_init(&fdmap[i].rwlock, NULL);
	}
	return SUCCESS;
}

// This function de-initializes the cache entries
status_t deinitializeCache() {
	int i;

	printf("\n De-initializing Cache\n");
        for(i=0;i<MAX_CACHE_ENTRIES;i++) {
                fdmap[i].fd = NULL; // Setting fd to NULL means entry is not in use
                pthread_rwlock_destroy(&fdmap[i].rwlock);
        }
        return SUCCESS;
}

// This function finds an unused cache entry and fills in file related information
status_t cacheFd(filename_t *fn, FILE *fd, char *openmode) {
	int cacheLoc = -1, i;

	// Find out where to insert information in fdmap[]
	pthread_mutex_lock(&cacheMutex);

        // Instead of using for() loop, this can be improved further to avoid for() loop
        // by calculating hash value for file name and then using a cache entry based
        // on the hash value
	for(i=0;i<MAX_CACHE_ENTRIES;i++) {
		// If an entry with fd=NULL is found, it means the entry is not in use
		if (fdmap[i].fd == NULL) {
			cacheLoc = i;
			fdcnt++;
			break;
		}
	}

	// Found an entry, save the information 
	fdmap[cacheLoc].fd = fd;
	strcpy(fdmap[cacheLoc].fn.fname, fn->fname);
        strcpy(fdmap[cacheLoc].openmode, openmode);
	pthread_mutex_unlock(&cacheMutex);

	printf("\n File information for file: %s saved in cache at cache location: %d", fn->fname, cacheLoc);
	return SUCCESS;
}

// This function removes a cached entry by setting the 'fd' field in the cache entry to NULL
status_t rmCacheFd(cache_t *entry) {
	printf("\n Removing file information from cache for file: %s", entry->fn.fname);
        pthread_mutex_lock(&cacheMutex);
	entry->fd = NULL;
	fdcnt--;
	pthread_mutex_unlock(&cacheMutex);
	return SUCCESS;
}

// This function helps to find an open FD for the given file name
cache_t* findFd(filename_t *fn) {
	int i;

	// This can be improved further by calculating hash value for file name
	// at the time of inserting FD information in cache. And then while searching for existing FD,
	// again calculate the hash value for file name and search the entry with the hash value
	printf("\n Searching cache entry for file name: %s", fn->fname);
	pthread_mutex_lock(&cacheMutex);
	for(i=0;i<MAX_CACHE_ENTRIES;i++) {
		// more checks can be added to compare file mode
		if (strcmp(fn->fname, fdmap[i].fn.fname) == 0) {
			printf("\n Found cache entry for file name: %s", fn->fname);
			pthread_mutex_unlock(&cacheMutex);
			return &fdmap[i];
		}
	}
	pthread_mutex_unlock(&cacheMutex);

	printf("\n Didn't find cache entry for file name: %s", fn->fname);
	return NULL;
}

// This function creates a file
status_t fCreate(req_t *request) {
    createfile_t *req = &(request->u.cr);
    filename_t *fn = &(request->fn);
    FILE* fd;
    int fname_len;
    status_t status;
    char *openmode = req->openmode;
    cache_t *entry;
    
    fname_len = strlen(fn->fname);
    if (fname_len==0)
	return FAILURE;

    // If FD already exists, return SUCCESS to reuse the FD.
    // This can be improved further to check existing openmode for
    // FD and if it can be allowed to be re-used based on openmode.
    entry = findFd(fn);
    if (entry != NULL) {
	printf("\n File with name: %s already exists", fn->fname);
        return SUCCESS;
    }

    // At a time only one file can be created
    // More checks can be added here to not allow two threads to create
    // file with same name in parallel
    pthread_mutex_lock(&createMutex);
    fd = fopen(fn->fname, openmode);
    pthread_mutex_unlock(&createMutex);

    if (fd != NULL) {
	printf("\n File created with name: %s", fn->fname);
	// Save information in cache
        status = cacheFd(fn, fd, openmode);
        // Assuming cacheFd() succeeded
        return SUCCESS;
    }
    return FAILURE;
}

// This function writes the input buffer contents to the file specified with file name
// The 'FD' related to the file should already be opened
status_t fWrite(req_t *request) {
    writefile_t *req = &(request->u.wr);
    filename_t *fn = &(request->fn);
    FILE* fd;
    int size, nitems=1;
    cache_t *cacheEntry;

    // Find FD for the given filename
    cacheEntry = findFd(fn);
    if (cacheEntry == NULL) {
        printf("\n File not opened/created by the program. Please open/create the file.");
        return FAILURE;
    }

    // Take write lock to ensure no one else can write to the same file
    pthread_rwlock_wrlock(&cacheEntry->rwlock);
    fseek(cacheEntry->fd, req->pos, req->whence);
    size = fwrite(req->writeBuf, req->reqWSize, nitems, cacheEntry->fd);
    pthread_rwlock_unlock(&cacheEntry->rwlock);

    if (size != nitems) {
	printf("\n Write failed for file: %s", fn->fname);
	return FAILURE;
    }
    req->resWSize = req->reqWSize;
    printf("\n Write succeeded to file: %s, %d bytes were written", fn->fname, req->reqWSize);
    return SUCCESS;
}

// This function reads the contents of the file specified with file name
// The 'FD' related to the file should already be opened
status_t fRead(req_t *request) {
    readfile_t *req = &(request->u.rd);
    filename_t *fn = &(request->fn);
    FILE* fd;
    int size, nitems=1;
    cache_t *cacheEntry;

    // Find FD for the given filename
    cacheEntry = findFd(fn);
    if (cacheEntry == NULL) {
        printf("\n File not opened/created by the program. Please open/create the file.");
        return FAILURE;
    }

    // Take read lock prior to reading from the file
    // More improvement can be done to let two threads read
    // from different positions in a file
    pthread_rwlock_rdlock(&cacheEntry->rwlock);
    fseek(cacheEntry->fd, req->pos, req->whence);
    size = fread(req->readBuf, req->reqRSize, nitems, cacheEntry->fd);
    pthread_rwlock_unlock(&cacheEntry->rwlock);

    if (size != nitems) {
        printf("\n Read failed for file: %s", fn->fname);
        return FAILURE;
    }
    printf("\n Read succeeded to file: %s, %d bytes were read", fn->fname, req->reqRSize);
    return SUCCESS;
}

// This function deletes the file with the specified name
status_t fDelete(req_t *request) {
    deletefile_t *req = &(request->u.df);
    filename_t *fn = &(request->fn);
    FILE* fd;
    int rc;
    cache_t *cacheEntry;

    // Find FD for the given filename
    cacheEntry = findFd(fn);
    if (cacheEntry == NULL) {
        printf("\n The program doesn't have information about the file: %s", fn->fname);
        return FAILURE;
    }

    // Take write lock to ensure no one else can write to/delete the same file
    pthread_rwlock_wrlock(&cacheEntry->rwlock);
    rc = remove(fn->fname);
    pthread_rwlock_unlock(&cacheEntry->rwlock);

    if(rc != 0) {
	printf("\n Delete failed for file: %s", fn->fname);
	return FAILURE;
    }

    printf("\n Delete succeeded for file: %s", fn->fname);

    // Clear the related cache entry
    rmCacheFd(cacheEntry);
    return SUCCESS;
}

// This function gets invoked when a thread gets created
void* workerThread(void *arg) {
      req_t *request;

      // if lookForWork=1 then thread will look for request to handle
      while(lookForWork) {
	      pthread_mutex_lock(&queueMutex);
	      if (queuedRequests == 0) {
			pthread_mutex_unlock(&queueMutex);
			// if no work available then continue looking for work
			continue;
	      }

	      // We got work, we just save the pointer to the request. We don't
	      // need to copy the entire request as we assume in real world, prior to
	      // reaching this code, the request would have been already copied prior
	      // to it being added to the reqQueue.
	      request = reqQueue[qReadPtr];
	      qReadPtr++;
      	      queuedRequests--;
	      if (qReadPtr == MAX_QUEUE_SIZE)
			qReadPtr = 0;
	      pthread_mutex_unlock(&queueMutex);	

	      // Handle the request
	      switch(request->type) {
        	    case CREATE:
			printf("\n\n Processing CREATE request");
	                if (fCreate(request))
        	            printf("\n Create request failed for file: %s", request->fn.fname);
                	else
	                    printf("\n Create request succeeded for file: %s", request->fn.fname);
        	    break;

           	    case WRITE:
			printf("\n\n Processing WRITE request");
	                if (fWrite(request))
        	            printf("\n Write request failed for file: %s", request->fn.fname);
                	else
	                    printf("\n Write request succeeded for file: %s", request->fn.fname);
        	    break;

	            case READ:
			printf("\n\n Processing READ request");
                	if (fRead(request))
                            printf("\n Read request failed for file: %s", request->fn.fname);
	                else
        	            printf("\n Read request succeeded for file: %s, read data: %s", request->fn.fname, request->u.rd.readBuf);
	            break;

	            case DELETE:
			printf("\n\n Processing DELETE request");
                	if (fDelete(request))
	                    printf("\n Delete request failed for file: %s", request->fn.fname);
        	        else
                	    printf("\n Delete request succeeded for file: %s", request->fn.fname);
	            break;

		    default:
			printf("\n Invalid request type");
 	       }
	} // end of while()
}

void myMiniServer() {
    int sockfd, clientfd, currSampleReq=0, totalSampleReqs=6, i;
    req_t *request;
    char *buffer, readbuf[MAX_RW_SIZE];
    pthread_t tid[MAX_THREADS];
    
    // Sample test requests
    req_t testSampleReq[6] ={ {CREATE, sizeof(createfile_t), "myfile", .u={.cr= { "w+"}}},
	                {CREATE, sizeof(createfile_t), "myfile", .u={.cr= { "w+"}}},
			{WRITE, sizeof(writefile_t), "myfile", .u={.wr= {0, SEEK_CUR, 10, 0, "hellohello"}}},
                        {WRITE, sizeof(writefile_t), "myfile", .u={.wr= {0, SEEK_CUR, 10, 0, "worldworld"}}},
			{READ, sizeof(readfile_t), "myfile", .u={.rd= {0, SEEK_SET, 10, readbuf}}},
			{DELETE, sizeof(deletefile_t), "myfile", .u={.df= { }}} };


    printf("\n USING MINI SERVER");
    pthread_mutex_init(&cacheMutex, NULL);
    pthread_mutex_init(&createMutex, NULL);
    pthread_mutex_init(&queueMutex, NULL);

    // Initialize cache
    initCache();

    // use socket() to get socket fd - sockfd
    // use bind() to bind to local port
    // use listen() to listen for connection requests
    // use accept() to accept a connection - save new fd returned by accept() in clientfd
    
    // Set lookForWork=1 so that newly created threads would start looking for work
    lookForWork = 1;

    // Create threads
    for (i=0;i<MAX_THREADS;i++)
	pthread_create(&tid[i], NULL, workerThread, (void*)request);

    // Now check for requests
    while(1) {
	// use recv() to get buffer(request) for socket fd- clientfd
	// recv(clientfd, buffer, len)
	// save the buffer
	
	// As socket related code to handle requests from
	// client is not implemented, then for testing
	// lets use the testSampleReq[] which has some sample
	// test requests. And totalSampleReqs is count of sample
	// requests.
	buffer = (char*)&testSampleReq[currSampleReq++];
        request = ((req_t*)buffer);

	switch(request->type) {
            case CREATE:
            case WRITE:
            case READ:
            case DELETE:
		// If queue is full then we will have to keep looking
		// for an empty slot in queue.
		// This can be improved to avoid using while(1).
		// Also can improve to make use of condition variable
		// and pthread_cond_signal() to signal
		// threads about availability of work
                while (1) {
			pthread_mutex_lock(&queueMutex);
			if (queuedRequests != MAX_QUEUE_SIZE) {
				if (qWritePtr == MAX_QUEUE_SIZE)
					qWritePtr = 0;

				// queue the request
				reqQueue[qWritePtr] = request;	
        	                queuedRequests++;
                	        qWritePtr++;
				pthread_mutex_unlock(&queueMutex);
				break;
			}
			pthread_mutex_unlock(&queueMutex);
		} // end of while()
	    break;
	
	    default:
		printf("\n Invalid request type");
	}

	// Used sleep() just for testing purpose, so that requests are
	// handled in sequence.
	sleep(1); // Remove sleep() when requests are read from socket.

	// Remove below lines of code when not testing
	// As we are using sample test requests, we want to come out of the
	// while() loop when all requests are handled
        if (currSampleReq==totalSampleReqs) {
		printf("\n\n We have handle all sample requests");
	 	break;       
	}
    } // end of while()

    // Setting lookForWork=0, this would tell threads to stop looking for work
    lookForWork = 0;

    // Join threads
    for(i=0;i<MAX_THREADS; i++)
	pthread_join(tid[i], NULL);

    pthread_mutex_destroy(&cacheMutex);
    pthread_mutex_destroy(&createMutex);
    pthread_mutex_destroy(&queueMutex);

    deinitializeCache();
    printf("\n EXITING MINI SERVER");
}

int main()
{
    // Start mini server
    myMiniServer();
    return 0;
}

