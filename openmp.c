/*
OMP Implementation
Parralelising recBitonicSort, impBitonicSort
*/


#include <stdio.h>
#include <time.h>
#include <stdlib.h>
#include <omp.h>
#include <sys/time.h> //for real time

#define MAX_THREADS 256
#define MIN_Q 12
#define MAX_Q 24
#define MIN_P 0
#define MAX_P 8


struct timeval startwtime, endwtime;
double seq_time;

int activeThreads = 0,maxThreads;

omp_lock_t writelock;


int N;          // data array size
int *a;         // data array to be sorted

const int ASCENDING  = 1;
const int DESCENDING = 0;

void init(void);
void print(void);
void sort(void);
void test(void);
inline void exchange(int i, int j);
inline void compare(int i, int j, int dir);
void bitonicMerge(int lo, int cnt, int dir);
void recBitonicSort(int lo, int cnt, int dir);
void impBitonicSort(void);
int cmpfunc_ascending (const void * a, const void * b);
int cmpfunc_descending (const void * a, const void * b);


int main(int argc, char **argv) {
  
  int p,q;

  
  srand(time(NULL));

  if(argc !=3) {
    printf("How to use: %s p q\n where p is the 2^p no. of threads\n where 2^q is problem size (power of two)\n", argv[0]);
    exit(1);
  }

  
  p = atoi(argv[1]);

  maxThreads = 1 << p;

  if ( ( maxThreads < 1 ) || ( maxThreads > MAX_THREADS ) ) {
    printf("p should be between %d and %d.\n", MIN_P, MAX_P);
    exit(2);
  }

  
  q = atoi(argv[2]);

  if( ( q < 12 ) || ( q > 24 ) ){
    printf("q should be between %d and %d.\n", MIN_Q, MAX_Q);
    exit(3);
  }

  N = 1 << q;
  
  //creating array in main thread
  a = (int *) malloc(N * sizeof(int));

  //Imperative OMP sort
  init();

  printf("\nImperative OMP Implementation:\n");
  gettimeofday (&startwtime, NULL);
  impBitonicSort();
  gettimeofday (&endwtime, NULL);

  seq_time = (double)((endwtime.tv_usec - startwtime.tv_usec)/1.0e6
          + endwtime.tv_sec - startwtime.tv_sec);

  printf("Imperative OMP Wall clock time = %f\n", seq_time);

  test();

  //Recursive OMP Parallel sort
  omp_init_lock(&writelock);

  init();

  printf("\nRecursive OMP Parralel Implementation:\n");
  gettimeofday (&startwtime, NULL);
  sort();
  gettimeofday (&endwtime, NULL);

  omp_destroy_lock(&writelock);

  seq_time = (double)((endwtime.tv_usec - startwtime.tv_usec)/1.0e6 + endwtime.tv_sec - startwtime.tv_sec);
  printf("Parallel OMP Recursive Wall clock time: %f\n", seq_time);

  test();

  //Qsort sort
  init();
 
  printf("\nqsort Implementation:\n");
  gettimeofday (&startwtime, NULL);
  qsort(a, N, sizeof(int), cmpfunc_ascending );
  gettimeofday (&endwtime, NULL);
  
  seq_time = (double)((endwtime.tv_usec - startwtime.tv_usec)/1.0e6 + endwtime.tv_sec - startwtime.tv_sec);
  printf("qsort Wall clock time: %f\n", seq_time);

  test();

  return 0;
  
}

/** -------------- SUB-PROCEDURES  ----------------- **/ 

/** procedure test() : verify sort results **/
void test() {
  int pass = 1;
  int i;
  for (i = 1; i < N; i++) {
    pass &= (a[i-1] <= a[i]);
  }

  printf("TEST %s\n",(pass) ? "PASSed" : "FAILed");
}


/** procedure init() : initialize array "a" with data **/
void init() {
  int i;
  //resetting thread usage to zero for next implementation
  activeThreads = 0;
  for (i = 0; i < N; i++) {
    a[i] = rand() % N; // (N - i);
  }
}

/** procedure  print() : print array elements **/
void print() {
  int i;
  for (i = 0; i < N; i++) {
    printf("%d\n", a[i]);
  }
  printf("\n");
}


/** INLINE procedure exchange() : pair swap **/
inline void exchange(int i, int j) {
  int t;
  t = a[i];
  a[i] = a[j];
  a[j] = t;
}



/** procedure compare() 
   The parameter dir indicates the sorting direction, ASCENDING 
   or DESCENDING; if (a[i] > a[j]) agrees with the direction, 
   then a[i] and a[j] are interchanged.
**/
inline void compare(int i, int j, int dir) {
  if (dir==(a[i]>a[j])) 
    exchange(i,j);
}




/** Procedure bitonicMerge() 
   It recursively sorts a bitonic sequence in ascending order, 
   if dir = ASCENDING, and in descending order otherwise. 
   The sequence to be sorted starts at index position lo,
   the parameter cnt is the number of elements to be sorted. 
 **/
void bitonicMerge(int lo, int cnt, int dir) {
  if ( cnt > 1 ) {
    int k = cnt / 2 ;
    int i;
    for (i=lo; i<(lo+k); i++)
      compare(i, i+k, dir);
    bitonicMerge(lo,k,dir); 
    bitonicMerge(lo+k,k,dir);
  }
}



/** function recBitonicSort() 
    first produces a bitonic sequence by recursively sorting 
    its two halves in opposite sorting orders, and then
    calls bitonicMerge to make them in the same order 
 **/
void recBitonicSort(int lo, int cnt, int dir) {

  if ( cnt > 1 ) {
    int k = cnt / 2;
    int parallel = 0;
    //if the subarrays are less than 100 elements long
    //we use q sort for faster sorting
    if( (k-lo)>100 ){
      qsort(a+lo, k, sizeof(int), cmpfunc_ascending );
      qsort(a+lo+k, k, sizeof(int), cmpfunc_descending );
    }
    else if (activeThreads < maxThreads - 2) {
      omp_set_lock(&writelock);
      activeThreads += 2 ;
      omp_unset_lock(&writelock);

      //the remaining parallelism needs to run in the available threads
      omp_set_num_threads(maxThreads - activeThreads);

      #pragma omp parallel
      {
        #pragma omp sections
        {
          #pragma omp section
            recBitonicSort(lo, k, ASCENDING);

          #pragma omp section 
            recBitonicSort(lo+k, k, DESCENDING);
        }
      }
      parallel = 1;
    }
    //Child threads will cease to exist once its completed
    //so we measure this descreasion
    if(parallel){
        omp_set_lock(&writelock);
        activeThreads-=2;
        omp_unset_lock(&writelock);
    }
    else{
        qsort(a + lo, k, sizeof(int), cmpfunc_ascending );
        qsort(a + lo + k, k, sizeof(int), cmpfunc_descending );
    }
    bitonicMerge(lo, cnt, dir); 
    
  }  
}


/** function sort() 
   Caller of recBitonicSort for sorting the entire array of length N 
   in ASCENDING order
 **/
void sort() {
  recBitonicSort(0, N, ASCENDING);
}


/*
  imperative version of bitonic sort
*/

void impBitonicSort() {

  int i,j,k;
  omp_set_num_threads(maxThreads - activeThreads);
  int chunk = (N/(maxThreads - activeThreads) );
  for (k=2; k<=N; k=2*k) {
    for (j=k>>1; j>0; j=j>>1) {
      #pragma omp parallel for schedule(static,chunk)
      for (i=0; i<N; i++) {
  int ij=i^j;
  if ((ij)>i) {
    if ((i&k)==0 && a[i] > a[ij]) 
        exchange(i,ij);
    if ((i&k)!=0 && a[i] < a[ij])
        exchange(i,ij);
  }
      }
    }
  }
}


/*for qsort*/
int cmpfunc_ascending (const void * a, const void * b) {
   return ( *(int*)a - *(int*)b );
}
int cmpfunc_descending (const void * a, const void * b) {
   return ( *(int*)b - *(int*)a );
}
//from: https://www.tutorialspoint.com/c_standard_library/c_function_qsort.htm