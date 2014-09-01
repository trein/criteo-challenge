import csv
import constants

from sklearn import linear_model
from sklearn.externals import joblib


MODEL_FILENAME = 'criteo/data/model.pkl'


class Trainer(object):
    def __init__(self, load=False):
        if load:
            self.clf = joblib.load(MODEL_FILENAME)
        else:
            self.clf = linear_model.SGDRegressor()

    @constants.timed
    def train(self):
        with open(constants.TRAIN_EXPANDED, 'r') as source:
            reader = csv.reader(source)
            next(reader, None)

            n_sample = 0
            labels = []
            features = []
            for feature_vector in reader:
                # ad_id = feature_vector[0]
                labels.append(feature_vector[1])
                features.append(feature_vector[2:])

                n_sample += 1
                if n_sample % 50000 == 0:
                    self.clf.partial_fit(features, labels)
                    print 'Processing sample [%s]' % n_sample

        print 'Finished training'
        print 'Estimator parameters [%s]' % self.clf.get_params()

        # saving model into file
        joblib.dump(self.clf, MODEL_FILENAME, compress=9)


def main():
    Trainer().train()


if __name__ == '__main__':
    main()
