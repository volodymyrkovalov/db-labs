#include <stdio.h>
#include <stdlib.h>
#include <io.h>

//TODO: correct names

const long int RECORDS_PER_UNIT = 50;
const long int MAX_DELETION = 10;
const int Q = 435345345; //hash constant
const int H = 354353445; //hash constant
const int FIELD_LENGTH = 20;

char im[25] = "index_master.db";
char is[25] = "index_slave.db";
char tm[25] = "table_master.db";
char ts[25] = "table_slave.db";
char mm[25] = "manager_master.db";
char ms[25] = "manager_slave.db";


int hash(const char* str) {
    int hash = 0;
    for (int i = 0; (i < FIELD_LENGTH-1) && str[i]; ++i)
        hash = (H * hash + str[i]) % Q;
    return hash % Q;
}

typedef struct  {
    char id[20];
    char name[20];
    char surname[20];
    char age[3];
} User;

void user_print(const User* a) {
    printf("(hash = %d) %s - ", hash(a->id), a->id);
    printf("%s - ", a->name);
    printf("%s - ", a->surname);
    printf("%s\n", a->age);
}

typedef struct {
    char id[20];
    char date[20];
    char text[20];
    unsigned long int main_pk;
} Post;



void post_print(const Post* f) {
    printf("(hash = %d) %s - ", hash(f->id), f->id);
    printf("%s - ", f->date);
    printf("%s - ", f->text);
    printf("%d\n", f->main_pk);
}

typedef struct {
    unsigned char present; // 1 - present, 0 - deleted
    unsigned long int PK;
    long int ind;
    long int aux_pos; //auxiliary index - for quicker search; for main item -> index of head subitrem list, for sub -> index text next in list
} IndexCell;

typedef struct {
    FILE* file_table; //table
    IndexCell* tind;
    long int deletion_manager;
    long int capacity;
    long int total;
    long int count;
} Table;
Table users, posts;

void endl() { printf("\n"); }

void insertion_sort(long int* array, const long int len) {
    int j;
    for (int i = 0; i < len; ++i) {
        j = i;
        while (j && array[j - 1] > array[j]) {
            long int swap = array[j - 1];
            array[j - 1] = array[j];
            array[j] = swap;
            j--;
        }
    }
}

long int l_binary_search(const long int* array, const long int key, const long int left, const long int right) {
    if (left == right-1) {
        if (key < array[right]) {
            return left;
        }
        return right;
    }

    long int m = (long int)((left + right) / 2);

    if (array[m] < key)
        l_binary_search(array, key, m, right);
    else
        l_binary_search(array, key, left, m);
}

void read_db(FILE* fd, unsigned char* buf, const size_t size, const long int offset_records) {
    fseek(fd, size * offset_records, SEEK_SET);
    fread(buf, size, 1, fd);
}

void write_db(FILE* fd, unsigned char* buf, const size_t size, const long int offset_records) {
    fseek(fd, size * offset_records, SEEK_SET);
    fwrite(buf, size, 1, fd);
    fflush(fd);
}

void clean_m() {
    IndexCell* new_tind = (IndexCell*)malloc(users.capacity * sizeof(IndexCell));
    long int j = 0;
    for (long int i = 0; i < users.total; ++i) {
        if (users.tind[i].present == 1) {
            new_tind[j] = users.tind[i];
            ++j;
        }
    }
    free(users.tind);
    users.tind = new_tind;

    users.deletion_manager = 0;
    users.total = users.count;
}

void clean_s() {
    IndexCell* new_tind = (IndexCell*)malloc(posts.capacity * sizeof(IndexCell));
    long int delete_count = posts.total - posts.count;
    long int* delete_pos = (long int*)malloc(delete_count * sizeof(long int));

    long int j = 0;
    long int k = 0;
    for (long int i = 0; i < posts.total; ++i) {
        if (posts.tind[i].present == 1) {
            new_tind[j] = posts.tind[i];
            ++j;
        }
        else {
            delete_pos[k] = i;
            ++k;
        }
    }
    insertion_sort(delete_pos, posts.total - posts.count);

    for (long int i = 0; i < posts.count; ++i) {
        if (new_tind[i].aux_pos != -1) {
            long int shift = l_binary_search(delete_pos, new_tind[i].aux_pos, 0, delete_count - 1);
            if (new_tind[i].aux_pos > delete_pos[0])
                ++shift;
            new_tind[i].aux_pos = new_tind[i].aux_pos - shift;
        }
    }
    for (long int i = 0; i < users.total; ++i)
        if ((users.tind[i].present != 0) && (users.tind[i].aux_pos!=-1)) {
            long int shift = l_binary_search(delete_pos, users.tind[i].aux_pos, 0, delete_count - 1);
            if (users.tind[i].aux_pos > delete_pos[0])
                ++shift;
            users.tind[i].aux_pos = users.tind[i].aux_pos - shift;
        }

    free(delete_pos);
    free(posts.tind);
    posts.tind = new_tind;

    posts.deletion_manager = 0;
    posts.total = posts.count;
}

FILE* open_db_data(const char* file_name, const long int size) {
    FILE* fd;
    fopen_s(&fd, file_name, "rb+");
    if (fd == NULL) {
        fopen_s(&fd, file_name, "wb+");
        _chsize_s(_fileno(fd), size * RECORDS_PER_UNIT);
    }
    return fd;
}

void open_mng_info(const char* file_name, Table* t) {
    FILE* fd;
    fopen_s(&fd, file_name, "rb");
    if (fd == NULL) {
        t->count = t->total = t->deletion_manager = 0;
        t->capacity = RECORDS_PER_UNIT;
    }
    else {
        read_db(fd, &t->count, sizeof(long int), 0);
        read_db(fd, &t->deletion_manager, sizeof(long int), 1);
        t->total = t->count;

        t->capacity = ((long int)(t->total / RECORDS_PER_UNIT) + 1) * RECORDS_PER_UNIT;

        fclose(fd);
    }
}

void open_db_find(const char* file_name, Table* t) {
    t->tind = (IndexCell*)malloc(t->capacity * sizeof(IndexCell));

    FILE* fd;
    fopen_s(&fd, file_name, "rb");
    if (fd != NULL) {
        for (long int i = 0; i < t->total; ++i)
            read_db(fd, &t->tind[i], sizeof(IndexCell), i);

        fclose(fd);
    }
}

void open_db() {
    users.file_table = open_db_data(tm, sizeof(User));
    open_mng_info(mm, &users);
    open_db_find(im, &users);

    posts.file_table= open_db_data(ts, sizeof(Post));
    open_mng_info(ms, &posts);
    open_db_find(is, &posts);
}

void remember_mng_info(const char* file_name, Table* t) {
    FILE* fd;
    fopen_s(&fd, file_name, "wb+");

    write_db(fd, &t->count, sizeof(long int), 0);
    write_db(fd, &t->deletion_manager, sizeof(long int), 1);

    fclose(fd);
}

void remember_tind(const char* file_name, Table* t) {
    FILE* fd;
    fopen_s(&fd, file_name, "wb+");
    if (fd != NULL) {
        for (long int i = 0; i < t->total; ++i)
            write_db(fd, &t->tind[i], sizeof(IndexCell), i);

        fclose(fd);
        free(t->tind);
    }
}

void close_db() {
    if (users.count < users.total)
        clean_m();
    if (posts.count < posts.total)
        clean_s();

    remember_mng_info(mm, &users);
    remember_tind(im, &users);
    fclose(users.file_table);

    remember_mng_info(ms, &posts);
    remember_tind(is, &posts);
    fclose(posts.file_table);
}

void change_capacity(Table* t) {
    t->capacity += RECORDS_PER_UNIT;
    IndexCell* new_tind = (IndexCell*)malloc(t->capacity * sizeof(IndexCell));
    memcpy(new_tind, t->tind, t->total * sizeof(IndexCell));

    free(t->tind);
    t->tind = new_tind;

    fflush(t->file_table);

    size_t size = t == &users ? sizeof(User) : sizeof(Post);
    _chsize_s(_fileno(t->file_table), size * t->capacity);
}

void print_tind(const Table* t) {
    printf("INDEX TABLE: pos - present - PK - index - auxiliary pos\n");
    for (int i = 0; i < t->total; ++i)
        printf("%d - %d - %d - %d - %d\n",
               i, t->tind[i].present, t->tind[i].PK, t->tind[i].ind, t->tind[i].aux_pos);
}

void print_data_m() {
    printf("USERS: DATA: name - surname - age"); endl();
    for (int i = 0; i < users.count; ++i) {
        User a;
        read_db(users.file_table, &a, sizeof(User),i);

        printf("--------------------------------------------------------------------------------------------------"); endl();
        printf("%d - ", i);
        user_print(&a);
    }
}

void print_data_s() {
    printf("POSTS: DATA: index - id - date - text - main item PK"); endl();
    for (int i = 0; i < posts.count; ++i) {
        Post p;
        read_db(posts.file_table, &p, sizeof(Post), i);

        printf("--------------------------------------------------------------------------------------------------"); endl();
        printf("%d - ", i);
        post_print(&p);
    }
}

long int add_to_sublist(const long int pos_main, const long int pos_sub) {
    long int next_sub = users.tind[pos_main].aux_pos;
    users.tind[pos_main].aux_pos = pos_sub;

    return next_sub;
}

void shift(const long int pos) {
    for (int i = 0; i < users.total; ++i)
        if ((users.tind[i].present == 1) && (users.tind[i].aux_pos >= pos))
            ++users.tind[i].aux_pos;

    for(int i=0;i<posts.total;++i)
        if ((posts.tind[i].present == 1) && (posts.tind[i].aux_pos >= pos))
            ++posts.tind[i].aux_pos;
}

void adjust_mng_info_after_insert(Table* t) {
    if (t->total == t->count)
        ++t->total;
    ++t->count;

    if (t->total >= t->capacity)
        change_capacity(t);
}

void adjust_mng_info_after_removal(Table* t) {
    t->count--;
    t->deletion_manager++;
}

long int get_insert_index(const Table* t) {
    return t->count;
}

long int* get_substitute_indexes(const Table* t, const long int delete_count, long int* delete_ind) {
    long int i = 0;
    long int j = 0;
    long int k = 0;

    long int* result = (long int)malloc(delete_count*sizeof(long int));
    while (i < delete_count) {
        if (t->count - 1 - k != delete_ind[delete_count - 1 - j]) {
            result[i] = t->count - 1 - k;
            ++i;
        }
        else
            ++j;
        ++k;
    }
    return result;
}

//returns first non-lesser
long int binary_search(const IndexCell* tind, const unsigned long int key, const long int left, const long int right) {
    if (left >= right) {
        if(tind[left].PK<key)
            return left+1;

        return left;
    }

    long int m = (long int)((left + right) / 2);
    if (tind[m].PK == key)
        return m;

    if (tind[m].PK < key)
        binary_search(tind, key, m+1, right);
    else
        binary_search(tind, key, left, m-1);
}

long int insert_to_tind(Table* t, const IndexCell* cell) {
    long int pos = 0;
    if (t->total == 0) {} 
    else { 
        pos = binary_search(t->tind, cell->PK, 0, t->total - 1);

        
        if ((pos < t->total)
            && (t->tind[pos].PK == cell->PK))
        {
            long int ptr = pos;

            while ((ptr < t->tind)
                   && (t->tind[ptr].PK == cell->PK) && (t->tind[ptr].present == 0))
                ++ptr;

            if((ptr < t->tind) && (t->tind[ptr].PK == cell->PK) && (t->tind[ptr].present != 0))
                return -1;
        }

        //if item's PK is in the middle, we should shift greater PKs
        if (pos < t->total) {
            if (t == &posts)
                shift(pos);
            for (int i = t->total; i > pos; --i)
                t->tind[i] = t->tind[i - 1];
        }
    }
    t->tind[pos] = *cell;
    adjust_mng_info_after_insert(t);

    return pos;
}

long int search_pos_tind(const Table* t, const unsigned long int key) {
    long int pos = binary_search(t->tind, key, 0, t->total - 1);

    if ((pos >= t->total)
        || ((pos < t->total) && (t->tind[pos].PK != key)))
    {
        printf("Error: No such item in database\n");
        pos = -1;
    }

    return pos;
}

long int search(const Table* t, const unsigned long int key) {
    long int pos = search_pos_tind(t, key);
    return pos != -1? t->tind[pos].ind : -1;
}

void get_m(const char* key) {
    long int ind = search(&users, hash(key));
    if (ind != -1) {
        User m;
        read_db(users.file_table, &m, sizeof(User), ind);
        user_print(&m);
    }
}

void get_s(const char* key) {
    long int ind = search(&posts, hash(key));
    if (ind != -1) {
        Post p;
        read_db(posts.file_table, &p, sizeof(Post), ind);
        post_print(&p);
    }
}

int insert_m(const char* info[]) {
    long int ind = get_insert_index(&users);

    User m;
    strcpy(m.id, info[0]);
    strcpy(m.name, info[1]);
    strcpy(m.surname, info[2]);
    strcpy(m.age, info[3]);

    unsigned long int h = hash(m.id);

    IndexCell cell;
    cell.present = 1;
    cell.PK = h;
    cell.ind = ind;
    cell.aux_pos = -1;

    int pos = insert_to_tind(&users, &cell);
    if (pos == -1) {
        printf("Error: There's an item with same PK\n");
        return -1;
    }
    write_db(users.file_table, &m, sizeof(User), ind);

    return 0;
}

int insert_s(const char* info[]) {
    long int ind = get_insert_index(&posts);

    Post p;
    strcpy(p.id, info[0]);
    strcpy(p.date, info[1]);
    strcpy(p.text, info[2]);

    unsigned long int h = hash(p.id);
    unsigned long int main_h = hash(info[3]);

    long int pos_main = search_pos_tind(&users, main_h);
    if (pos_main == -1)
        return -1;
    p.main_pk = main_h;

    IndexCell cell;
    cell.present = 1;
    cell.PK = h;
    cell.ind = ind;
    cell.aux_pos = -1;
    int pos = insert_to_tind(&posts, &cell);
    if (pos == -1) {
        printf("Error: There's an item with same PK or main item was not found\n");
        return -1;
    }

    posts.tind[pos].aux_pos = add_to_sublist(pos_main, pos);
    write_db(posts.file_table, &p, sizeof(Post),ind);

    return 0;
}

int edit_m(const char* key, const char* new_info[]) {
    long int ind = search(&users, hash(key));
    User a;
    read_db(users.file_table, &a, sizeof(User), ind);

    strcpy(a.name, new_info[0]);
    strcpy(a.surname, new_info[1]);
    strcpy(a.age, new_info[2]);

    write_db(users.file_table, &a, sizeof(User), ind);
}


int edit_s(const char* key, const char* new_info[]) {
    long int ind = search(&posts, hash(key));
    Post f;
    read_db(posts.file_table, &f, sizeof(Post), ind);

    strcpy(f.date, new_info[0]);
    strcpy(f.text, new_info[1]);

    write_db(posts.file_table, &f, sizeof(Post), ind);
}

int delete_m(char* key) {
    long int pos = search_pos_tind(&users, hash(key));
    if (pos == -1)
        return -1;

    users.tind[pos].present = 0;
    long int delete_master[] = { users.tind[pos].ind };
    long int* subst_master = get_substitute_indexes(&users, 1, delete_master);
    if (delete_master[0] < users.count - 1) {
        User m;
        read_db(users.file_table, &m, sizeof(User), subst_master[0]);
        write_db(users.file_table, &m, sizeof(User), delete_master[0]);
        long int subst_pos_master = search_pos_tind(&users, hash(m.name));
        users.tind[subst_pos_master].ind = delete_master[0];
    }
    adjust_mng_info_after_removal(&users);


    long int slave_count = 0;
    long int ptr = users.tind[pos].aux_pos;
    while (ptr != -1) {
        ++slave_count;
        ptr = posts.tind[ptr].aux_pos;
    }

    //actually remember their indexes
    long int* delete_indexes = (long int*)malloc(sizeof(long int) * slave_count);
    ptr = users.tind[pos].aux_pos;
    int i = 0;
    while (ptr != -1) {
        delete_indexes[i] = posts.tind[ptr].ind;
        posts.tind[ptr].present = 0;
        ++i;
        ptr = posts.tind[ptr].aux_pos;
    }
    insertion_sort(delete_indexes, slave_count);

    long int* subst_indexes = get_substitute_indexes(&posts, slave_count, delete_indexes);

    Post p;
    long int final_count = posts.count - slave_count;
    for (int i = 0; i < slave_count; ++i) {
        if (delete_indexes[i] < final_count) {
            read_db(posts.file_table, &p, sizeof(Post), subst_indexes[i]);
            write_db(posts.file_table, &p, sizeof(Post), delete_indexes[i]);
            long int sub_pos = search_pos_tind(&posts, hash(p.id));
            posts.tind[sub_pos].ind = delete_indexes[i];
        }
        adjust_mng_info_after_removal(&posts);
    }

    free(subst_master);
    free(delete_indexes);
    free(subst_indexes);

    if (users.deletion_manager >= MAX_DELETION)
        clean_m();
    if (posts.deletion_manager >= MAX_DELETION)
        clean_s();

    return 0;
}

int delete_s(char* key) {
    long int pos = search_pos_tind(&posts, hash(key));
    if (pos == -1)
        return -1;

    Post p;
    read_db(posts.file_table, &p, sizeof(Post), posts.tind[pos].ind);

    long int main_pos = search_pos_tind(&users, p.main_pk);
    long int ptr = users.tind[main_pos].aux_pos;
    if (pos != ptr) {
        while (posts.tind[ptr].aux_pos != pos)
            ptr = posts.tind[ptr].aux_pos;
    }
    else {
        users.tind[main_pos].aux_pos = posts.tind[pos].aux_pos;
    }
    posts.tind[ptr].aux_pos = posts.tind[pos].aux_pos;
    posts.tind[pos].present = 0;

    //get substitute
    long int delete_ind[] = { posts.tind[pos].ind };
    long int* substitute = get_substitute_indexes(&posts, 1, delete_ind);

    //rewrite substitute on new position
    if (delete_ind[0] < posts.count - 1) {
        read_db(posts.file_table, &p, sizeof(Post), substitute[0]);
        write_db(posts.file_table, &p, sizeof(Post), delete_ind[0]);
        long int sub_pos = search_pos_tind(&posts, hash(p.id));
        posts.tind[sub_pos].ind = delete_ind[0];
    }

    adjust_mng_info_after_removal(&posts);
    free(substitute);

    if (posts.deletion_manager >= MAX_DELETION)
        clean_s();

    return 0;
}

void print_db() {
    printf("\n============================================ DATABASE ============================================\n");

    print_data_m();
    endl();
    print_tind(&users);
    endl(); endl();

    print_data_s();
    endl();
    print_tind(&posts);

    printf("==================================================================================================\n\n");
}

void interface() {
    char* info[4];
    for (int i = 0; i < 4; ++i) {
        info[i] = (char*)malloc(FIELD_LENGTH * sizeof(char));
    }
    char* key = (char*)malloc(FIELD_LENGTH * sizeof(char));

    int choice = 1;
    while ((choice > 0) && (choice<14)) {
        printf("Choose action:\n 1 = Get-m\n 2 = Get-s\n 3 = Insert-m\n 4 = Insert-s\n 5 = Edit-m\n 6 = Edit-s\n 7 = Delete-m\n 8 = Delete-s\n 9 = print database\n 0 = EXIT\n");
        scanf("%d", &choice);
        getchar();

        if (choice == 1) { //Get-m
            printf("Enter the key:\n");
            fgets(key, FIELD_LENGTH, stdin);
            key[strlen(key) - 1] = 0;

            get_m(key);
        }
        else if (choice == 2) { //Get-s
            printf("Enter the key:\n");
            fgets(key, FIELD_LENGTH, stdin);
            key[strlen(key) - 1] = 0;

            get_s(key);
        }
        else if (choice == 3) { //Insert-m
            printf("Enter the id(PK):\n");
            fgets(info[0], FIELD_LENGTH, stdin);
            info[0][strlen(info[0]) - 1] = 0;

            printf("Enter the name:\n");
            fgets(info[1], FIELD_LENGTH, stdin);
            info[1][strlen(info[1]) - 1] = 0;

            printf("Enter the surname:\n");
            fgets(info[2], FIELD_LENGTH, stdin);
            info[2][strlen(info[2]) - 1] = 0;

            printf("Enter the age:\n");
            fgets(info[3], FIELD_LENGTH, stdin);
            info[3][strlen(info[3]) - 1] = 0;

            insert_m(info);
        }
        else if (choice == 4) { //Insert-s
            printf("Enter the id(PK):\n");
            fgets(info[0], FIELD_LENGTH, stdin);
            info[0][strlen(info[0]) - 1] = 0;

            printf("Enter the date:\n");
            fgets(info[1], FIELD_LENGTH, stdin);
            info[1][strlen(info[1]) - 1] = 0;

            printf("Enter the text:\n");
            fgets(info[2], FIELD_LENGTH, stdin);
            info[2][strlen(info[2]) - 1] = 0;

            printf("Enter by which User(FK):\n");
            fgets(info[3], FIELD_LENGTH, stdin);
            info[3][strlen(info[3]) - 1] = 0;

            insert_s(info);
        }
        else if (choice == 5) { //Edit-m
            printf("Enter the key:\n");
            fgets(key, FIELD_LENGTH, stdin);
            key[strlen(key) - 1] = 0;

            printf("Enter the new name(PK):\n");
            fgets(info[0], FIELD_LENGTH, stdin);
            info[0][strlen(info[0]) - 1] = 0;

            printf("Enter the new surname:\n");
            fgets(info[1], FIELD_LENGTH, stdin);
            info[1][strlen(info[1]) - 1] = 0;

            printf("Enter the new age:\n");
            fgets(info[2], FIELD_LENGTH, stdin);
            info[2][strlen(info[2]) - 1] = 0;

            edit_m(key, info);
        }
        else if (choice == 6) { //Edit-s
            printf("Enter the key:\n");
            fgets(key, FIELD_LENGTH, stdin);
            key[strlen(key) - 1] = 0;

            printf("Enter new date:\n");
            fgets(info[0], FIELD_LENGTH, stdin);
            info[0][strlen(info[0]) - 1] = 0;

            printf("Enter new text:\n");
            fgets(info[1], FIELD_LENGTH, stdin);
            info[1][strlen(info[1]) - 1] = 0;

            edit_s(key, info);
        }
        else if (choice == 7) { //Delete-m
            printf("Enter the key:\n");
            fgets(key, FIELD_LENGTH, stdin);
            key[strlen(key) - 1] = 0;

            delete_m(key);
        }
        else if (choice == 8) { //Delete-s
            printf("Enter the key:\n");
            fgets(key, FIELD_LENGTH, stdin);
            key[strlen(key) - 1] = 0;

            delete_s(key);
        }
        else if (choice == 9) {
            print_db();
        }
        endl();
    }
    for(int i=0;i<4;++i)
        free(info[i]);
    free(key);
}

int main() {
    open_db();
    print_db();

    interface();

    close_db();

    system("pause");
    return 0;
}