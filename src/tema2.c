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

struct TorrentFile {
    char filename[MAX_FILENAME];
    char chunks[MAX_CHUNKS][HASH_SIZE];
    int chunk_nr;
};

struct Ownership {
    struct TorrentFile file;
    int* tasks;
    int numtasks;
};

int search_file(struct Ownership ownerships[], int n, char filename[]) {
    for (int i = 0; i < n; i++) {
        if (strcmp(ownerships[i].file.filename, filename) == 0) {
            return i;
        }
    }

    return -1;
}

void *download_thread_func(void *arg)
{
    int rank = *(int*) arg;

    return NULL;
}

void *upload_thread_func(void *arg)
{
    int rank = *(int*) arg;

    return NULL;
}

void tracker(int numtasks, int rank) {
    int copy_numtasks = numtasks - 1;
    MPI_Status status;

    //  Initialize starter file info
    struct Ownership ownerships[MAX_FILES];
    int size = 0;

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
            char chunks[MAX_CHUNKS][HASH_SIZE];
            for (int j = 0; j < chunk_nr; j++) {
                MPI_Recv((void *) chunks[j], HASH_SIZE, MPI_CHAR, status.MPI_SOURCE, 0, MPI_COMM_WORLD, &status);
            }

            //  Tracker checks if it has this file
            int index = search_file(ownerships, size, filename);

            //  Add new file
            if (index == -1) {
                //  Set file info
                strcpy(ownerships[size].file.filename, filename);
                ownerships[size].file.chunk_nr = chunk_nr;
                for (int j = 0; j < chunk_nr; j++) {
                    strcpy(ownerships[size].file.chunks[i], chunks[j]);
                }

                //  Add its first owner
                ownerships[size].tasks = malloc(numtasks * sizeof(int));
                ownerships[size].tasks[0] = status.MPI_SOURCE;
                ownerships[size].numtasks = 1;
                
                size++;

            //  File exists; add only the owner
            } else {
                ownerships[index].tasks[ownerships[index].numtasks] = status.MPI_SOURCE;
                ownerships[index].numtasks++;
            }
        }
        copy_numtasks--;
    }

    for (int i = 0; i < size; i++) {
        printf("%s: ", ownerships[i].file.filename);
        for (int j = 0; j < ownerships[i].numtasks; j++) {
            printf("%d, ", ownerships[i].tasks[j]);
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
            MPI_Send((void *)owned_files[i].chunks[j], HASH_SIZE, MPI_CHAR, TRACKER_RANK, 0, MPI_COMM_WORLD);
        }
    }

    //  Thread init
    pthread_t download_thread;
    pthread_t upload_thread;
    void *status;
    int r;

    r = pthread_create(&download_thread, NULL, download_thread_func, (void *) &rank);
    if (r) {
        printf("Eroare la crearea thread-ului de download\n");
        exit(-1);
    }

    r = pthread_create(&upload_thread, NULL, upload_thread_func, (void *) &rank);
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
