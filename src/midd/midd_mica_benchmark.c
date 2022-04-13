/*
 * @Author: your name
 * @Date: 2022-03-09 20:00:04
 * @LastEditTime: 2022-03-09 21:04:46
 * @LastEditors: Please set LastEditors
 * @Description: 打开koroFileHeader查看配置 进行设置: https://github.com/OBKoro1/koro1FileHeader/wiki/%E9%85%8D%E7%BD%AE
 * @FilePath: /LineKV/src/mica_kv/benchmark.c
 */

#include "hash.h"
#include "dhmp.h"
#include "dhmp_log.h"
#include "midd_mica_benchmark.h"

int __test_size;
int __access_num=0;
int read_num, update_num;
int end_round=0;
int op_gaps[4];
int little_idx;
bool is_all_set_all_get =false;

int main_node_is_readable;
enum WORK_LOAD_DISTRIBUTED workload_type;
struct test_kv kvs_group[TEST_KV_NUM];

const double A = 1.3;  
const double C = 1.0;  
//double pf[TEST_KV_NUM]; 
// int rand_num[TEST_KV_NUM]={0};

int * read_num_penalty=NULL;

// 生成符合Zipfian分布的数据
void generate_zipfian(double pf[], size_t nums)
{
    int i;
    double sum = 0.0;
 
    for (i = 0; i < nums; i++)
        sum += C/pow((double)(i+2), A);

    for (i = 0; i < nums; i++)
    {
        if (i == 0)
            pf[i] = C/pow((double)(i+2), A)/sum;
        else
            pf[i] = pf[i-1] + C/pow((double)(i+2), A)/sum;
    }
}

// 根据Zipfian分布生成索引
void pick_zipfian(double pf[], int rand_num[], int max_num)
{
	int i, index;

    generate_zipfian(pf, max_num);

    srand(time(0));
    for ( i= 0; i < max_num; i++)
    {
        index = 0;
        double data = (double)rand()/RAND_MAX; 
        while (index<(max_num)&&data > pf[index])   
            index++;
		rand_num[i]=index;
       // printf("%d ", rand_num[i]);
    }
    //printf("\n");
}

void pick_uniform(double pf[], int rand_num[], int max_num)
{
	int i, rand_idx, tmp;

    for (i=0 ;i<max_num; i++)
        rand_num[i] = i;

    srand(time(0));
    for ( i= 0; i < max_num; i++)
    {
        rand_idx = rand() % max_num; 
        tmp = rand_num[i];
        rand_num[i] = rand_num[rand_idx];
        rand_num[rand_idx] = tmp;
    }
}

struct test_kv *
generate_test_data(size_t key_offset, size_t val_offset, size_t value_length, size_t kv_nums)
{
    size_t i,j;
    int partition_id;
    //struct test_kv *kvs_group;
    //kvs_group = (struct test_kv *) malloc(sizeof(struct test_kv) * kv_nums);
    memset(kvs_group, 0, sizeof(struct test_kv) * kv_nums);

    for (i = 0; i < kv_nums; i++)
    {
        size_t key = i;
        key = key << 16;

        kvs_group[i].true_key_length = sizeof(key);
        kvs_group[i].true_value_length = value_length;
        kvs_group[i].key = (uint8_t *)malloc(kvs_group[i].true_key_length);
        kvs_group[i].value = (uint8_t*) malloc(kvs_group[i].true_value_length);
        kvs_group[i].key_hash = hash(kvs_group[i].key, kvs_group[i].true_key_length );

        // 注意我们 get 回来的数据需要  考虑到 header 和 tail 的大小
        for (j=0; j<1; j++) // 暂时只开一个缓冲区
            kvs_group[i].get_value[j] = (uint8_t*) malloc(kvs_group[i].true_value_length + VALUE_HEADER_LEN + VALUE_TAIL_LEN);

        memset(kvs_group[i].value, (int)(i+val_offset), kvs_group[i].true_value_length);
        memcpy(kvs_group[i].key, &key, kvs_group[i].true_key_length);

        partition_id = *((size_t*)kvs_group[i].key)  % (PARTITION_NUMS);
    }

    return kvs_group;
}

	// size_t 	 out_value_length; 	// 返回值
	// uint32_t out_expire_time;	// 返回值
	// bool	 partial;			// 返回值
	// uint8_t  out_value[0];		// 返回值

// 我们不回去比较key，因为如果value可以正确拿到，则key一定是正确的（另外我们没用拿key的接口)
bool 
cmp_item_value(size_t a_value_length, const uint8_t *a_out_value, size_t b_value_length,const uint8_t *b_out_value)
{
    bool re = true;
    if (a_value_length != b_value_length)
    {
        ERROR_LOG("MICA value length error! %lu != %lu", a_value_length, b_value_length);
        re= (false);
    }

#ifdef DUMP_MEM
    size_t off = 0;
    bool first = false, second = false, second_count=0;
    for (off = 0; off < b_value_length; off++)
    {
        if (a_out_value[off] != b_out_value[off])
        {
            if (first == false)
            {
                first = true;
                size_t tp = off - 16;
                for (; tp < off; tp++)
                    printf("%ld, %hhu, %hhu\n", tp, a_out_value[tp], b_out_value[tp]);
            }
            // 打印 unsigned char printf 的 格式是 %hhu
            printf("%ld, %hhu, %hhu\n", off, a_out_value[off], b_out_value[off]);
        }
        else
        {
            if (first == true && second == false && second_count < 16)
            {
                printf("%ld, %hhu, %hhu\n", off, a_out_value[off], b_out_value[off]);
                second_count ++;
                if (second_count == 16)
                    second = true;
            }
        }
    }
#endif

    if (memcmp(a_out_value, b_out_value, b_value_length) != 0 )
    {
        ERROR_LOG("value context error! %p, %p, len is %lu", a_out_value, b_out_value, b_value_length);
        re=  (false);
    }

    return re;
}

void dump_value_by_addr(const uint8_t * value, size_t value_length)
{
    uint64_t header_v, tail_v, value_count;
    
    bool dirty;

    header_v = *(uint64_t*) value;
    value_count = *(uint64_t*) (value + sizeof(uint64_t));
    tail_v = *(uint64_t*) (value + 2*sizeof(uint64_t) + GET_TRUE_VALUE_LEN(value_length));
    dirty = *(bool*)(value + 3*sizeof(uint64_t) + GET_TRUE_VALUE_LEN(value_length));

    // INFO_LOG("value header_v is %lu, value_count is %lu, tail_v is %lu, dirty is %d", header_v,value_count,tail_v, dirty);
#ifdef DUMP_VALUE
    const uint8_t * value_base  = (value + 2 * sizeof(uint64_t));
    HexDump(value_base, (int)(GET_TRUE_VALUE_LEN(value_length)), (int) value_base);
#endif
}


bool 
cmp_item_all_value(size_t a_value_length, const uint8_t *a_out_value, size_t b_value_length,const uint8_t *b_out_value)
{
    bool re = true;
    if (a_value_length != b_value_length)
    {
        ERROR_LOG("MICA value length error! %lu != %lu", a_value_length, b_value_length);
        re= (false);
    }
    size_t off = 0;
    for (off = 0; off < b_value_length; off++)
    {
        if (a_out_value[off] != b_out_value[off])
        {
            // 打印 unsigned char printf 的 格式是 %hhu
            printf("%ld, %hhu, %hhu\n", off, a_out_value[off], b_out_value[off]);
        }
    }
    if (memcmp(a_out_value, b_out_value, b_value_length) != 0 )
    {
        ERROR_LOG("value context error! %p, %p, len is %lu", a_out_value, b_out_value, b_value_length);

        // dump_value_by_addr(a_out_value, a_value_length);
        // dump_value_by_addr(b_out_value, b_value_length);
        re=  (false);
    }

    if (memcmp(GET_TRUE_VALUE_ADDR(a_out_value), GET_TRUE_VALUE_ADDR(b_out_value), GET_TRUE_VALUE_LEN(b_value_length)) != 0 )
    {
        ERROR_LOG("true value error!");
        re=  (false);
    }
    return re;
}

// struct BOX* box[PARTITION_MAX_NUMS];
struct dhmp_msg** set_msgs_group;

// void DO_READ()
// {
// if(box->needRTT == 0)
// 	// normal_time_count_start;
// 	;
// 	//TODO:read from mica
// if(box->needRTT == 0)
// 	//normal_time_count_over;
// 	;
// else 
// 	//special_time_count_over;
// 	return;
// }

/***
 * Whale: kind = 0 ;
 * CRAQ: kind = 1;
 * CHT: kind = 2;
 *   current_node_number = [0 ~ total_node_number-1]
 * return NULL means NO penalty;
*/
struct BOX* intial_box(int current_node_number, int total_node_number, int kind)
{
	int times = 0;
	if(current_node_number == 0)
		return Empty_pointer;
	switch (kind)
	{
		case 0: times = (current_node_number - 1) + 2;
		break;
		case 1: times = (total_node_number - current_node_number)*2;
		break;
		case 2: times = 2;
		break;
		default :
			return Empty_pointer;
	}
	times = (times)/2;//1.5==>1
	
	struct BOX * box = (struct BOX*) malloc(sizeof(struct BOX));
	box->array = (TYPE_BOX *)malloc(times * sizeof(TYPE_BOX));
	memset(box->array, 0, times * sizeof(TYPE_BOX));
	box->length = times;
	box->cur = -1;
	box->total = 0;
	box->needRTT = 0;
	return box; 
}

/***
 * value is acquired from each write operation
*/
void update_box(TYPE_BOX value, struct BOX * box)
{
	//find the same value already in the box and delete it
	int i;
	for(i =0;i < box->length;i++)
	{
		if(box->array[i] == value)
		{
			box->array[i] = 0;
			box->total --;
			break;
		}
	}
	// put new value in box
	box->cur = (box->cur + 1) % box->length;
	if(box->array[box->cur] != 0)
		box->total --;
	box->array[box->cur] = value;
	box->total ++;
	return;
}



void print_box(struct BOX* box)
{
	int i = 0;
	for(;i < box->length;i++)
		printf("%d ",box->array[i]);
	printf("cur = %d,length = %d %d\n",box->cur,box->length,box->total);
}



// int main()
// {
// 	TYPE_BOX value[10] = {1, 1, 3, 4 ,5 ,1 , 1, 3 ,4 ,5};
// 	struct BOX* box = intial_box(1,7,1);
// 	if(box == Empty_pointer)
// 		return 0; 
		
// 	//test
// 	// to define  rand_read_set , rand_write_set and length of rand_write_set
// 	int need_read = 0, read_in_this_term;
// 	current_read = &(rand_read_set[0]);

// 	for(i in length of rand_write_set) // each write operation trigger a update_box (a term)
// 	{
// 		update_box(rand_write_setet[i], box);
// 		need_read += each read_set;
// 		read_in_this_term = finish_read(current_read, need_read, box);
// 		current_read = current_read + read_in_this_term;
// 		need_read = need_read - read_in_this_term;
// 		//wait for next write
// 	}
// 	if (need_read != 0)
// 	{
// 		wait a rtt  then:
// 			finish_read(current_read, need_read, box);
// 	}


// 	// print_box(box);
// 	// update_box(1,box);
// 	// update_box(5,box);
// 	// update_box(3,box);
// 	// update_box(5,box);
// 	// update_box(6,box);
// 	// update_box(1,box);
// 	// update_box(5,box);
// 	// print_box(box);
// 	// printf("finish = %d\n",finish_read(value, 10, box));
// 	// print_box(box);
// 	// update_box(5,box);
// 	// update_box(3,box);
// 	// print_box(box);
// 	// printf("finish = %d\n",finish_read(value, 10, box));
// 	// print_box(box);
// 	// printf("finish = %d\n",finish_read(value, 10, box));
// 	// print_box(box);
	
	
// 	free(box->array);
// 	free(box);
// 	return 0;
// }