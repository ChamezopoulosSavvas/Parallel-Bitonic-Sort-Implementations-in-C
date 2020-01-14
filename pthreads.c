/*
Pthread implementation
Parralelising recBitonicSort

Chamezopoulos Savvas
*/



#include <stdio.h>
#include <time.h>
#include <stdlib.h>
#include <pthread.h>
#include <sys/time.h> //for real time
#include <math.h>

#define MAX_THREADS 256
#define MIN_Q 12
#define MAX_Q 24
#define MIN_P 0
#define MAX_P 8


struct timeval startwtime, endwtime;
double seq_time;

pthread_attr_t pthread_custom_attr;
int activeThreads = 0, maxThreads;

pthread_mutex_t mux;

typedef struct {
  int lo;
  int cnt;
  int dir;
} parm;

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
void* recBitonicSort(void *);
parm wrapper(int lo, int cnt, int dir);
parm unwrapper(void *);
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
  maxThreads = pow(2,p);

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

  
  pthread_mutex_init (&mux, NULL);
  pthread_attr_init(&pthread_custom_attr);

  init();

  printf("\nRecursive Parralel Pthread Implementation:\n");
  gettimeofday (&startwtime, NULL);
  sort();

  
  
  gettimeofday (&endwtime, NULL);

  pthread_attr_destroy(&pthread_custom_attr);
  pthread_mutex_destroy (&mux);

  seq_time = (double)((endwtime.tv_usec - startwtime.tv_usec)/1.0e6 + endwtime.tv_sec - startwtime.tv_sec);
  printf("Parallel Recursive Pthread Wall clock time: %f\n", seq_time);

  test();

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
void* recBitonicSort(void *arg) {

  //followed the exact same strategy as in bitonicMerge
  parm prev = unwrapper((void *)arg);
  pthread_t f1,f2;

  if ( prev.cnt > 1 ) {
    int k = prev.cnt / 2;

    int parallel = 0;
    //if the subarrays are less than 100 elements long
    //we use q sort for faster sorting
    if( (k-prev.lo)>100 ){
      qsort(a+prev.lo, k, sizeof(int), cmpfunc_ascending );
      qsort(a+prev.lo+k, k, sizeof(int), cmpfunc_descending );
    }
    else if (activeThreads < (maxThreads - 2) ) {
      
      pthread_mutex_lock (&mux);
      activeThreads += 2 ;
      pthread_mutex_unlock (&mux);

      parm lower = wrapper(prev.lo, k, ASCENDING);
      parm higher = wrapper(prev.lo + k, k, DESCENDING);

      pthread_create(&f1, &pthread_custom_attr, recBitonicSort, (void *)&lower); 
      pthread_create(&f2, &pthread_custom_attr, recBitonicSort, (void *)&higher);
      
      parallel = 1;
    }

    if(parallel){

      pthread_join(f1,NULL);  
      pthread_join(f2,NULL);
      pthread_mutex_lock (&mux);
      activeThreads -= 2;
      pthread_mutex_unlock (&mux);
      
    }
    else {
      //using qsort for 
      //sequetial sorting in the same thread
      qsort(a + prev.lo, k, sizeof(int), cmpfunc_ascending );
      qsort(a + prev.lo + k, k, sizeof(int), cmpfunc_descending );
    
  }
    
    bitonicMerge(prev.lo, prev.cnt, prev.dir);
    
  }
}


/** function sort() 
   Caller of recBitonicSort for sorting the entire array of length N 
   in ASCENDING order
 **/
void sort() {
  parm prev = wrapper(0, N, ASCENDING);
  recBitonicSort((void *)&prev);
}


/* function wrapper

takes a vars of type (int) and transforms it into type parm

handles variables of type struct parm as a regular variable
without the need for many recurring instructions of the same format in
the rest of the code

*/
parm wrapper(int lo, int cnt, int dir){
  parm retval;
  
  retval.lo = lo;
  retval.cnt = cnt;
  retval.dir = dir;

  return retval;
}


/* function unwrapper

takes a var of type (void *) and transforms it into type parm

handles variables of type struct parm as a regular variable
without the need for many instructions of the same type

*/
parm unwrapper(void *arg){
  parm p;
  
  p.lo = ((parm *) arg)->lo;
  p.cnt = ((parm *) arg)->cnt;
  p.dir = ((parm *) arg)->dir;

  return p;
}


/*for qsort*/
int cmpfunc_ascending (const void * a, const void * b) {
   return ( *(int*)a - *(int*)b );
}
int cmpfunc_descending (const void * a, const void * b) {
   return ( *(int*)b - *(int*)a );
}
//from: https://www.tutorialspoint.com/c_standard_library/c_function_qsort.htm