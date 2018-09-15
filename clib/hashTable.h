//
// Created by george on 20/7/2018.
//

#ifndef EFFICIENTOPERATORS_HASHTABLE_H
#define EFFICIENTOPERATORS_HASHTABLE_H

#include <cstdlib>
#include <climits>
#include <cstring>

// HashTable with integers as (key, value) for incremental distinct
struct ht_node {
    char status; // 1B
    int key;     // 4B
    int counter; // 4B  => 9B
    //int value;
    //char padding[3];
    //void operator=(ht_node1 const&) { }
};

struct hashtable {
    int size;
    // int capacity;
    struct ht_node *table;
    //hashtable & operator=(hashtable const&) { }
};

inline int ht_hash (const int size, const int key) {
    return (key & (size - 1));
}

inline int round (int value) {
    if (!(value && (!(value&(value-1))))) {
        value--;
        value |= value >> 1;
        value |= value >> 2;
        value |= value >> 4;
        value |= value >> 8;
        value |= value >> 16;
        value++;
    }
    return value;
}

hashtable* ht_create (int size) {
    // rounding up to next power of 2
    size = round(size);
    hashtable *map;
    map = (hashtable*) malloc(sizeof( hashtable));
    map->size = size;
    map->table = (ht_node *)  malloc( sizeof(ht_node) * size );
    for (int i = 0; i < size; i++)
        memset(&map->table[i], 0, sizeof(ht_node));

    return map;
}

void ht_insert (ht_node * table, const int key/*char * key*/, const int value, const int size) {
    int hashIndex = ht_hash(size, key);
    //find next free space -> use two for loops
    while (table[hashIndex].status
        && table[hashIndex].key != key) {
        hashIndex++;
        hashIndex %= size;
    }
    table[hashIndex].status = 1;
    table[hashIndex].key = key;    //strcpy(map->table[hashIndex].key, key);
    //table[hashIndex].value = value;
    table[hashIndex].counter++;
}

void ht_insert_and_increment (ht_node * table, const int key, const int value, const int size) {
    int ind = ht_hash(size, key), i = ind;
    char tempStatus;
    for (; i < size; i++) {
        tempStatus = table[i].status;
        if (tempStatus && table[i].key == key) { //update
            //table[i].value += value;
            table[i].counter++;
            return;
        }
        if (!tempStatus) { // first insert
            table[i].status = 1;
            table[i].key = key;//strcpy(table[hashIndex].key, key);
            //table[i].value = value;
            table[i].counter++;
            return;
        }
    }
    for (i = 0; i < ind; i++) {
        tempStatus = table[i].status;
        if (tempStatus && table[i].key == key) {
            //table[i].value += value;
            table[i].counter++;
            return;
        }
        if (tempStatus) {
            table[i].status = 1;
            table[i].key = key;//strcpy(table[hashIndex].key, key);
            //table[i].value = value;
            table[i].counter++;
            return;
        }
    }
}

void ht_update_value (ht_node * table, const int key/*char * key*/, const int value, const int size) {
    int hashIndex = ht_hash(size, key);
    while(table[hashIndex].status) {
        //if node found
        if(table[hashIndex].key == key){
            //table[hashIndex].value = value;
        }
        hashIndex++;
        hashIndex %= size;
    }
}

void ht_update_counter (ht_node * table, const int key/*char * key*/, const int count, const int size) {
    /*int hashIndex = ht_hash(size, key);

    while(map->table[hashIndex].status) {
        //if node found
        if(map->table[hashIndex].key == key){//strcmp(map->table[hashIndex].key, key) == 0) {
            map->table[hashIndex].counter +=count;

            //if (map->table[hashIndex].counter == 0)
                //map->table[hashIndex].status = 0;
            return;
        }
        hashIndex++;
        hashIndex %= size;
    }*/
    int ind = ht_hash(size, key), i = ind;
    for (; i < size; i++) {
        if ((table[i].status) && table[i].key == key) {
            table[i].counter += count;
            return;
        }
    }
    for (i = 0; i < ind; i++) {
        if ((table[i].status) && table[i].key == key) {
            table[i].counter += count;
            return;
        }
    }
}

int ht_delete (ht_node * table, const int key/*char * key*/, const int size) {
    int ind = ht_hash(size, key), i = ind;
    for (; i < size; i++) {
        if ((table[i].status) && table[i].key == key) {
            int temp = table[i].counter; //value!!
            table[i].status = 0;
            return temp;
        }
    }
    for (i = 0; i < ind; i++) {
        if ((table[i].status) && table[i].key == key) {
            int temp = table[i].counter;
            table[i].status = 0;
            return temp;
        }
    }
    return INT_MIN;
}

int ht_get (ht_node * table, int key/*char * key*/, const int size) {
    int ind = ht_hash(size, key), i = ind;
    for (; i < size; i++) {
        if ((table[i].status) && table[i].key == key)
            return table[i].counter; //value!!
    }
    for (i = 0; i < ind; i++) {
        if ((table[i].status) && table[i].key == key)
            return table[i].counter;
    }
    return INT_MIN;
}

void ht_free (hashtable * map) {
    free(map->table);
    free(map);
}


// More generic implementation of HashTable for larger keys
#define KEY_SIZE 12
#define VALUE_SIZE 4 //(16 - 1 - KEY_SIZE)

union ht_value_c {
    int int_value;
    float  float_value;
    double double_value;
};

struct ht_node_c {
    char status;
    char key[KEY_SIZE];
    ht_value_c value;
    int counter;
    char padding[3];
};

struct hashtable_c {
    int size;
    struct ht_node_c *table;
};

// unsigned long
inline int ht_hash_c (const int size, const char * key) {
    /*unsigned */int hashval = 0;
    int i = KEY_SIZE-1;
    /* Convert our string to an integer */
    while (i >= 0 /*strlen(key)*/) { // little-endian
        hashval = hashval << 8;
        hashval += (unsigned char) key[i];
        i--;
    }
    //unsigned int hashval = 0, g;
    //register char *p;   /* pointer used to scan and access string */

    /* Convert our string to an integer using Peter Weinberger's algorithm */
    /*for (p = key; *p; p++) {
        hashval = (hashval << 4) + *p;
        *//* assumes a 32 bit int size *//*
        if ((g = hashval & 0xf0000000)) {
            hashval ^= g >> 24;
            hashval ^= g;
        }
    }*/
    //printf ("%d \n", hashval);
    return (hashval & (size - 1));
}

hashtable_c* ht_create_c (int size) {
    // rounding up to next power of 2
    size = round(size);
    hashtable_c *map;
    map = (hashtable_c*) malloc(sizeof( hashtable_c));
    map->size = size;
    map->table = (ht_node_c *)  malloc( sizeof(ht_node_c) * size );
    for (int i = 0; i < size; i++)
        memset(&map->table[i], 0, sizeof(ht_node_c));
    return map;
}

void ht_insert_c (ht_node_c * table, char * key, const ht_value_c value, const int size) {
    int hashIndex = ht_hash_c(size, key);
    //find next free space -> use two for loops
    while (table[hashIndex].status
        && memcmp(table[hashIndex].key, key, KEY_SIZE) != 0) {
        hashIndex++;
        hashIndex %= size;
    }
    table[hashIndex].status = 1;
    memcpy(table[hashIndex].key, key, KEY_SIZE);//strcpy(table[hashIndex].key, key);
    table[hashIndex].value = value;
    table[hashIndex].counter++;
}

float ht_insert_and_increment_c (ht_node_c * table, char * key, const float value, const int size) {
    int ind = ht_hash_c(size, key), i = ind;
    char tempStatus;
    for (; i < size; i++) {
        tempStatus = table[i].status;
        if (tempStatus && memcmp(table[i].key, key, KEY_SIZE) == 0) { //update
            table[i].value.float_value += value;
            table[i].counter++;
            return (table[i].value.float_value / (float) table[i].counter);
        }
        if (!tempStatus) { // first insert
            table[i].status = 1;
            memcpy(table[i].key, key, KEY_SIZE);//strcpy(table[hashIndex].key, key);
            table[i].value.float_value = value;
            table[i].counter++;
            return value;
        }
    }
    for (i = 0; i < ind; i++) {
        tempStatus = table[i].status;
        if (tempStatus && memcmp(table[i].key, key, KEY_SIZE) == 0) {
            table[i].value.float_value += value;
            table[i].counter++;
            return (table[i].value.float_value / (float) table[i].counter);
        }
        if (tempStatus) {
            table[i].status = 1;
            memcpy(table[i].key, key, KEY_SIZE);//strcpy(table[hashIndex].key, key);
            table[i].value.float_value = value;
            table[i].counter++;
            return value;
        }
    }

    return 0;
}

void ht_increment_value_c (ht_node_c * table, char * key, const float value, const int size) {
    int ind = ht_hash_c(size, key), i = ind;
    for (; i < size; i++) {
        if ((table[i].status) && memcmp(table[i].key, key, KEY_SIZE) == 0) {
            table[i].value.float_value += value;
            table[i].counter++;
            return;
        }
    }
    for (i = 0; i < ind; i++) {
        if ((table[i].status) && memcmp(table[i].key, key, KEY_SIZE) == 0) {
            table[i].value.float_value += value;
            table[i].counter++;
            return;
        }
    }
}

void ht_decrement_value_c (ht_node_c * table, char * key, const float value, const int size) {
    int ind = ht_hash_c(size, key), i = ind;
    for (; i < size; i++) {
        if ((table[i].status) && memcmp(table[i].key, key, KEY_SIZE) == 0) {
            table[i].value.float_value -= value;
            table[i].counter--;
            return;
        }
    }
    for (i = 0; i < ind; i++) {
        if ((table[i].status) && memcmp(table[i].key, key, KEY_SIZE) == 0) {
            table[i].value.float_value -= value;
            table[i].counter--;
            return;
        }
    }
}

void ht_free_c (hashtable_c * map) {
    free(map->table);
    free(map);
}


#endif //EFFICIENTOPERATORS_HASHTABLE_H
