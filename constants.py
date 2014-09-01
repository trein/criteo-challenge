import time
import datetime


BASE_DIR = '/Users/trein/datasets/criteo/'
# BASE_DIR = 'criteo/'

CATEGORY_MAPPING_OUT = BASE_DIR + 'data/categorical_mapping.json'
CATEGORY_STATUS_OUT = BASE_DIR + 'data/categorical_status.json'

TRAIN_RAW = BASE_DIR + 'train.csv'
TRAIN_EXPANDED = BASE_DIR + 'train_expanded.csv'

TEST_RAW = BASE_DIR + 'test.csv'
TEST_EXPANDED = BASE_DIR + 'test_expanded.csv'

LIMIT_CATEGORICAL = 15
INTEGER_INDEXES = xrange(2, 15)
INTEGER_IDS = ['I%s' % i for i in range(1, 14)]

CATEGORIES_INDEXES = xrange(15, 41)
CATEGORIES_IDS = ['C%s' % i for i in range(1, 27)]


def convert_train_c_id_to_index(c_id):
    num_id = int(c_id.lower().replace('c', ''))
    return CATEGORIES_INDEXES[num_id - 1]


def convert_train_i_id_to_index(i_id):
    num_id = int(i_id.lower().replace('i', ''))
    return INTEGER_INDEXES[num_id - 1]


def convert_test_c_id_to_index(c_id):
    return convert_train_c_id_to_index(c_id) - 1


def convert_test_i_id_to_index(i_id):
    return convert_train_i_id_to_index(i_id) - 1


def timed(fn):
    def wrapped(*arg, **kw):
        ts = time.time()
        result = fn(*arg, **kw)
        te = time.time()
        delta = te-ts
        duration = datetime.timedelta(seconds=delta)
        print 'Function [%s] took [%s]' % (fn.__name__, duration)
        return result

    return wrapped
