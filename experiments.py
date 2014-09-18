import constants
import csv
import gzip


def balanced():
    # balanced data?
    with gzip.open(constants.TRAIN_EXPANDED, 'r') as source:
        reader = csv.reader(source)
        next(reader, None)

        n_samples = 0
        with_click = 0

        for feature_vector in reader:
            click = feature_vector[1]
            with_click = with_click + 1 if click == 1 else with_click
            n_samples += 1

        print 'Experiments results'
        print '-------------------'
        print 'Clicks [%s] out of [%s] - [%s] with click' % (with_click, n_samples, float(with_click) / n_samples)


def normalized():
    with gzip.open(constants.TRAIN_EXPANDED, 'r') as source:
        reader = csv.reader(source)
        next(reader, None)

        for feature_vector in reader:
            features = feature_vector[2:]
            for f in features:
                if float(f) > 1.0:
                    print 'Not normalized features'
                    print features
                    break


if __name__ == '__main__':
    # balanced()
    normalized()
