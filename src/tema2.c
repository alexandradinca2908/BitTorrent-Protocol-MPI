#include <mpi.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>

#define TRACKER_RANK 0
#define MAX_FILES 10
#define MAX_FILENAME 15
#define HASH_SIZE 32
#define MAX_CHUNKS 100
#define MAX_MSG_LEN 20
#define DOWNLOAD_TAG 1
#define UPLOAD_TAG 2

struct TorrentFile {
    char filename[MAX_FILENAME];
    char chunks[MAX_CHUNKS][HASH_SIZE + 1];
    int chunk_nr;
};

struct Swarm {
    struct TorrentFile file;
    int* seeds;
    int numseeds;
    int* peers;
    int numpeers;
};

struct ThreadData {
    int rank;
    int numtasks;
    int file_nr_o;
    int file_nr_w;
    char progress[MAX_CHUNKS][4];
    char crt_download[MAX_FILENAME];
    struct TorrentFile *owned_files;
    struct TorrentFile *wanted_files;
};

int search_file(struct Swarm swarms[], int n, char filename[]) {
    for (int i = 0; i < n; i++) {
        if (strcmp(swarms[i].file.filename, filename) == 0) {
            return i;
        }
    }

    return -1;
}

struct Swarm update_swarm(struct ThreadData *thread_data, int index) {
    struct Swarm swarm;
    char message[MAX_MSG_LEN] = "";
    int len = 0;
    int value = 0;

    MPI_Status status;

    //  Send swarm request
    strcpy(message, "swarmlist");
    MPI_Send((void *) message, strlen(message) + 1, MPI_CHAR, TRACKER_RANK, DOWNLOAD_TAG, MPI_COMM_WORLD);

    //  Wait for ack
    MPI_Recv((void *) message, 3, MPI_CHAR, TRACKER_RANK, DOWNLOAD_TAG, MPI_COMM_WORLD, &status);
    
    //  Request the swarm itself
    strcpy(message, thread_data->wanted_files[index].filename);
    MPI_Send((void *) message, strlen(message) + 1, MPI_CHAR, TRACKER_RANK, DOWNLOAD_TAG, MPI_COMM_WORLD);

    //  Wait for file name; in case the answer is "invalid", move on to next file
    MPI_Probe(TRACKER_RANK, DOWNLOAD_TAG, MPI_COMM_WORLD, &status);
    MPI_Get_count(&status, MPI_CHAR, &len);
    MPI_Recv((void *) message, len, MPI_CHAR, TRACKER_RANK, DOWNLOAD_TAG, MPI_COMM_WORLD, &status);
    strcpy(swarm.file.filename, message);

    if (strcmp(message, "invalid") == 0) {
        return swarm;
    }

    //  Chunk nr
    MPI_Recv((void *) &value, 1, MPI_INT, TRACKER_RANK, DOWNLOAD_TAG, MPI_COMM_WORLD, &status);
    swarm.file.chunk_nr = value;

    //  Chunks
    for (int i = 0; i < swarm.file.chunk_nr; i++) {
        MPI_Recv((void *) message, HASH_SIZE + 1, MPI_CHAR, TRACKER_RANK, DOWNLOAD_TAG, MPI_COMM_WORLD, &status);
    }

    //  Seed nr
    MPI_Recv((void *) &value, 1, MPI_INT, TRACKER_RANK, DOWNLOAD_TAG, MPI_COMM_WORLD, &status);
    swarm.numseeds = value;
    int seeds[thread_data->numtasks];
    swarm.seeds = seeds;

    //  Seeds
    for (int i = 0; i < swarm.numseeds; i++) {
        MPI_Recv((void *) &value, 1, MPI_INT, TRACKER_RANK, DOWNLOAD_TAG, MPI_COMM_WORLD, &status);
        swarm.seeds[i] = value;
    }

    //  Peer nr
    MPI_Recv((void *) &value, 1, MPI_INT, TRACKER_RANK, DOWNLOAD_TAG, MPI_COMM_WORLD, &status);
    swarm.numpeers = value;
    int peers[thread_data->numtasks];
    swarm.peers = peers;

    //  Peers
    for (int i = 0; i < swarm.numpeers; i++) {
        MPI_Recv((void *) &value, 1, MPI_INT, TRACKER_RANK, DOWNLOAD_TAG, MPI_COMM_WORLD, &status);
        swarm.peers[i] = value;
    }

    return swarm;
}

void *download_thread_func(void *arg)
{
    struct ThreadData *thread_data = (struct ThreadData*) arg;
    char message[MAX_MSG_LEN] = "";
    int len = 0;

    MPI_Status status;

    //  Start downloading wanted files
    for (int filei = 0; filei < thread_data->file_nr_w; filei++) {
        //  Acquire swarm for crt file
        struct Swarm swarm = update_swarm(thread_data, filei);

        //  Mark file as current download
        strcpy(thread_data->crt_download, swarm.file.filename);

        //  Init segment download progress
        for (int i = 0; i < swarm.file.chunk_nr; i++) {
            strcpy(thread_data->progress[i], "miss");
        }

        //  Start download
        int chunki = 0;
        while (chunki < swarm.file.chunk_nr) {
            //  Rotate between seed and peers, collecting segments

            for (int seed = 0; seed < swarm.numseeds; seed++) {
                //  Message: 'chunk' 'filename'
                sprintf(message, "%d", chunki);
                strcat(message,  " ");
                strcat(message, swarm.file.filename);

                //  Request chunk
                MPI_Send((void *) message, strlen(message) + 1, MPI_CHAR, swarm.seeds[seed], DOWNLOAD_TAG, MPI_COMM_WORLD);

                //  Receive answer
                MPI_Probe(swarm.seeds[seed], UPLOAD_TAG, MPI_COMM_WORLD, &status);
                MPI_Get_count(&status, MPI_CHAR, &len);
                MPI_Recv((void *) message, len, MPI_CHAR, TRACKER_RANK, UPLOAD_TAG, MPI_COMM_WORLD, &status);

                //  If segment is recieved, mark as ack and move to the next peer
                //  If answer is bad, just move to the next peer
                if (strcmp(message, "ack") == 0) {
                    strcpy(thread_data->progress[chunki], "ack");
                    chunki++;
                }

                //  Update swarm every 10 downloads
                if (chunki % 10 == 0) {
                    free(swarm.seeds);
                    free(swarm.peers);

                    swarm = update_swarm(thread_data, filei);
                }
            }

            for (int peer = 0; peer < swarm.numpeers; peer++) {
                //  Message: 'chunk' 'filename'
                sprintf(message, "%d", chunki);
                strcat(message,  " ");
                strcat(message, swarm.file.filename);

                //  Request chunk
                MPI_Send((void *) message, strlen(message) + 1, MPI_CHAR, swarm.peers[peer], DOWNLOAD_TAG, MPI_COMM_WORLD);

                //  Receive answer
                MPI_Probe(swarm.peers[peer], UPLOAD_TAG, MPI_COMM_WORLD, &status);
                MPI_Get_count(&status, MPI_CHAR, &len);
                MPI_Recv((void *) message, len, MPI_CHAR, TRACKER_RANK, UPLOAD_TAG, MPI_COMM_WORLD, &status);

                //  If segment is recieved, mark as ack and move to the next peer
                //  If answer is bad, just move to the next peer
                if (strcmp(message, "ack") == 0) {
                    strcpy(thread_data->progress[chunki], "ack");
                    chunki++;
                }

                //  Update swarm every 10 downloads
                if (chunki % 10 == 0) {
                    free(swarm.seeds);
                    free(swarm.peers);

                    swarm = update_swarm(thread_data, filei);
                }
            }
        }

        //  Notify tracker that the download is complete
        strcpy(swarm.file.filename);
        strcpy(" complete");
        MPI_Send((void *) message, strlen(message) + 1, MPI_CHAR, TRACKER_RANK, DOWNLOAD_TAG, MPI_COMM_WORLD);
    }

    //  Notify the tracker that download is entirely finished
    strcat(message, "done");
    MPI_Send((void *) message, strlen(message) + 1, MPI_CHAR, TRACKER_RANK, DOWNLOAD_TAG, MPI_COMM_WORLD);

    return NULL;
}

void *upload_thread_func(void *arg)
{
    struct ThreadData *thread_data = (struct ThreadData*) arg;

    return NULL;
}

void tracker(int numtasks, int rank) {
    int copy_numtasks = numtasks - 1;
    MPI_Status status;

    //  Initialize starter file info
    struct Swarm swarms[MAX_FILES];
    int swarm_size = 0;

    while (copy_numtasks > 0) {
        //  Recv number of files from a peer
        int file_nr;
        MPI_Recv((void *) &file_nr, 1, MPI_INT, MPI_ANY_SOURCE, 0, MPI_COMM_WORLD, &status);

        //  Recv each file and mark the process as peer
        //  For new files, set the chunks as well
        for (int i = 0; i < file_nr; i++) {
            //  Recv name size
            int namesize;
            MPI_Recv((void *) &namesize, 1, MPI_INT, status.MPI_SOURCE, 0, MPI_COMM_WORLD, &status);

            //  Recv name
            char filename[MAX_FILENAME];
            MPI_Recv((void *) filename, namesize, MPI_CHAR, status.MPI_SOURCE, 0, MPI_COMM_WORLD, &status);

            //  Recv number of chunks
            int chunk_nr;
            MPI_Recv((void *) &chunk_nr, 1, MPI_INT, status.MPI_SOURCE, 0, MPI_COMM_WORLD, &status);

            //  Recv chunks
            char chunks[MAX_CHUNKS][HASH_SIZE + 1];
            for (int j = 0; j < chunk_nr; j++) {
                MPI_Recv((void *) chunks[j], HASH_SIZE + 1, MPI_CHAR, status.MPI_SOURCE, 0, MPI_COMM_WORLD, &status);
            }

            //  Tracker checks if it has this file
            int index = search_file(swarms, swarm_size, filename);

            //  Add new file
            if (index == -1) {
                //  Set file info
                strcpy(swarms[swarm_size].file.filename, filename);
                swarms[swarm_size].file.chunk_nr = chunk_nr;
                for (int j = 0; j < chunk_nr; j++) {
                    strcpy(swarms[swarm_size].file.chunks[i], chunks[j]);
                }

                //  Add its first owner
                swarms[swarm_size].seeds = malloc(numtasks * sizeof(int));
                swarms[swarm_size].seeds[0] = status.MPI_SOURCE;
                swarms[swarm_size].numseeds = 1;
                swarms[swarm_size].peers = malloc(numtasks * sizeof(int));
                swarms[swarm_size].numpeers = 0;

                swarm_size++;

            //  File exists; add only the owner
            } else {
                swarms[index].seeds[swarms[index].numseeds] = status.MPI_SOURCE;
                swarms[index].numseeds++;
            }
        }
        copy_numtasks--;
    }

    //  Begin transfer
    copy_numtasks = numtasks - 1;
    while (copy_numtasks > 0) {
        char start = 's';
        MPI_Send((void *) &start, 1, MPI_CHAR, copy_numtasks, 0, MPI_COMM_WORLD);
        copy_numtasks--;
    }

    //  Loop until all downloads have finished
    int active_tasks = numtasks - 1;
    char message[MAX_MSG_LEN] = "";
    int len = 0;

    while (active_tasks > 0) {
        //  Wait for message
        MPI_Probe(MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
        MPI_Get_count(&status, MPI_CHAR, &len);
        MPI_Recv((void *) message, len, MPI_CHAR, status.MPI_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &status);

        if (strstr(message, "swarmlist")) {
            //  Confirm connection; wait for requested swarm list
            strcpy(message, "ack");
            MPI_Send((void *) message, 3, MPI_CHAR, status.MPI_SOURCE, status.MPI_TAG, MPI_COMM_WORLD);

            MPI_Probe(MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
            MPI_Get_count(&status, MPI_CHAR, &len);
            MPI_Recv((void *) message, len, MPI_CHAR, status.MPI_SOURCE, status.MPI_TAG, MPI_COMM_WORLD, &status);

            //  Give swarm for current file    
            int index = search_file(swarms, swarm_size, message);
                
            if (index == -1) {
                strcpy(message, "invalid");
                MPI_Send((void *) &message, strlen(message) + 1, MPI_CHAR, status.MPI_SOURCE, status.MPI_TAG, MPI_COMM_WORLD);
            } else {
                //  FILE
                //  Filename
                MPI_Send((void *) swarms[index].file.filename, strlen(swarms[index].file.filename) + 1, MPI_CHAR, status.MPI_SOURCE, status.MPI_TAG, MPI_COMM_WORLD);
                //  Chunk nr
                MPI_Send((void *) &swarms[index].file.chunk_nr, 1, MPI_INT, status.MPI_SOURCE, status.MPI_TAG, MPI_COMM_WORLD);
                //  Chunks
                for (int i = 0; i < swarms[index].file.chunk_nr; i++) {
                    MPI_Send((void *) swarms[index].file.chunks[i], HASH_SIZE + 1, MPI_CHAR, status.MPI_SOURCE, status.MPI_TAG, MPI_COMM_WORLD);
                }
                //  SWARM
                //  Seed nr
                MPI_Send((void *) &swarms[index].numseeds, 1, MPI_INT, status.MPI_SOURCE, status.MPI_TAG, MPI_COMM_WORLD);
                //  Seeds
                for (int i = 0; i < swarms[index].numseeds; i++) {
                    MPI_Send((void *) &swarms[index].seeds[i], 1, MPI_INT, status.MPI_SOURCE, status.MPI_TAG, MPI_COMM_WORLD);
                }
                //  Peer nr
                MPI_Send((void *) &swarms[index].numpeers, 1, MPI_INT, status.MPI_SOURCE, status.MPI_TAG, MPI_COMM_WORLD);
                //  Peers
                for (int i = 0; i < swarms[index].numpeers; i++) {
                    MPI_Send((void *) &swarms[index].peers[i], 1, MPI_INT, status.MPI_SOURCE, status.MPI_TAG, MPI_COMM_WORLD);
                }
            }                                 
        }
    }

    for (int i = 0; i < swarm_size; i++) {
        printf("%s: ", swarms[i].file.filename);
        for (int j = 0; j < swarms[i].numseeds; j++) {
            printf("%d, ", swarms[i].seeds[j]);
        }
        printf("\n");
    }
}

void peer(int numtasks, int rank) {
    //  Data init
    int rank_length = (int) log10((double) rank) + 1;

    char filename[MAX_FILENAME] = "in";
    char number[rank_length + 1];

    sprintf(number, "%d", rank);
    strcat(filename, number);
    strcat(filename, ".txt");

    //  Open read file
    FILE *fptr;
    fptr = fopen(filename, "r");

    if (fptr == NULL) {
        printf("Could not open file.\n");
        fclose(fptr);
        exit(-1);
    }

    //  Read owned files
    int file_nr_o = 0;
    fscanf(fptr, "%d\n", &file_nr_o);

    struct TorrentFile owned_files[file_nr_o];
    for (int i = 0; i < file_nr_o; i++) {
        //  Store name
        fscanf(fptr, "%s ", owned_files[i].filename);

        //  Store chunks
        fscanf(fptr, "%d\n", &owned_files[i].chunk_nr);
        for (int j = 0; j < owned_files[i].chunk_nr; j++) {
            fscanf(fptr, "%s\n", owned_files[i].chunks[j]);
        }
    }

    //  Read wanted files
    int file_nr_w = 0;
    fscanf(fptr, "%d\n", &file_nr_w);

    struct TorrentFile wanted_files[file_nr_w];
    for (int i = 0; i < file_nr_w; i++) {
        //  Store name
        fscanf(fptr, "%s\n", wanted_files[i].filename);
    }

    //  Close file
    fclose(fptr);

    //  SEND INFO TO TRACKER
    //  Send number of files
    MPI_Send((void *) &file_nr_o, 1, MPI_INT, TRACKER_RANK, 0, MPI_COMM_WORLD);

    for (int i = 0; i < file_nr_o; i++) {
        //  Send name size
        int namesize = strlen(owned_files[i].filename) + 1;
        MPI_Send((void *) &namesize, 1, MPI_INT, TRACKER_RANK, 0, MPI_COMM_WORLD);

        //  Send name
        MPI_Send((void *) owned_files[i].filename, namesize, MPI_CHAR, TRACKER_RANK, 0, MPI_COMM_WORLD);

        //  Send number of chunks
        MPI_Send((void *) &owned_files[i].chunk_nr, 1, MPI_INT, TRACKER_RANK, 0, MPI_COMM_WORLD);

        //  Send chunks
        for (int j = 0; j < owned_files[i].chunk_nr; j++) {
            //  Send chunk
            MPI_Send((void *)owned_files[i].chunks[j], HASH_SIZE + 1, MPI_CHAR, TRACKER_RANK, 0, MPI_COMM_WORLD);
        }
    }

    //  Wait for signal
    char start;
    MPI_Recv((void *) &start, 1, MPI_CHAR, TRACKER_RANK, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

    //  Thread init
    pthread_t download_thread;
    pthread_t upload_thread;
    void *status;
    int r;

    struct ThreadData thread_data;
    thread_data.rank = rank;
    thread_data.numtasks = numtasks;
    thread_data.file_nr_o = file_nr_o;
    thread_data.file_nr_w = file_nr_w;
    thread_data.owned_files = owned_files;
    thread_data.wanted_files = wanted_files;

    r = pthread_create(&download_thread, NULL, download_thread_func, (void *) &thread_data);
    if (r) {
        printf("Eroare la crearea thread-ului de download\n");
        exit(-1);
    }

    r = pthread_create(&upload_thread, NULL, upload_thread_func, (void *) &thread_data);
    if (r) {
        printf("Eroare la crearea thread-ului de upload\n");
        exit(-1);
    }

    r = pthread_join(download_thread, &status);
    if (r) {
        printf("Eroare la asteptarea thread-ului de download\n");
        exit(-1);
    }

    r = pthread_join(upload_thread, &status);
    if (r) {
        printf("Eroare la asteptarea thread-ului de upload\n");
        exit(-1);
    }
}
 
int main (int argc, char *argv[]) {
    int numtasks, rank;
 
    int provided;
    MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &provided);
    if (provided < MPI_THREAD_MULTIPLE) {
        fprintf(stderr, "MPI nu are suport pentru multi-threading\n");
        exit(-1);
    }
    MPI_Comm_size(MPI_COMM_WORLD, &numtasks);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    if (rank == TRACKER_RANK) {
        tracker(numtasks, rank);
    } else {
        peer(numtasks, rank);
    }

    MPI_Finalize();
}
