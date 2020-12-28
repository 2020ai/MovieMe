import logging
import os
from pyspark import SparkContext
from pyspark.mllib.recommendation import ALS


# logging details
log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
logging.basicConfig(level=logging.INFO, format=log_fmt)
logger = logging.getLogger(__name__)

def rating_count_avg(ID_and_ratings_tuple):
    '''Given a tuple of movieID and ratings
    returns counts and average of movies.
    '''

    num_of_ratings = len(ID_and_ratings_tuple[1])
    return ID_and_ratings_tuple[0], (num_of_ratings, float(sum(x for x in ID_and_ratings_tuple[1]))/num_of_ratings)


class MovieMe:
    '''A movie recommendation system using ALS.
    '''

    def __init__(self, dataset_path):
        '''Initialize the recommendation system given a dataset path.
        '''

        logger.info("Starting up the Recommendation System: ")
        self.sc = SparkContext.getOrCreate()

        # Load ratings data for later use
        logger.info("Getting the Ratings dataset.")
        ratings_file_path = os.path.join(dataset_path, 'ratings.csv')
        ratings_raw_RDD = self.sc.textFile(ratings_file_path)
        ratings_raw_data_header = ratings_raw_RDD.take(1)[0]
        self.ratings_RDD = ratings_raw_RDD.filter(lambda line: line != ratings_raw_data_header)\
            .map(lambda line: line.split(",")).map(lambda tokens: (int(tokens[0]), int(tokens[1]), float(tokens[2]))).cache()

        # Load movies data for later use
        logger.info("Getting Movies dataset.")
        movies_file_path = os.path.join(dataset_path, 'movies.csv')
        movies_raw_RDD = self.sc.textFile(movies_file_path)
        movies_raw_data_header = movies_raw_RDD.take(1)[0]
        self.movies_RDD = movies_raw_RDD.filter(lambda line: line != movies_raw_data_header)\
            .map(lambda line: line.split(",")).map(lambda tokens: (int(tokens[0]), tokens[1], tokens[2])).cache()
        self.movies_titles_RDD = self.movies_RDD.map(
            lambda x: (int(x[0]), x[1])).cache()

        # Hyperparameters for training the model
        self.rank = 8
        self.seed = 5
        self.iterations = 10
        self.regularization_parameter = 0.1
        
    def ratings_count_avg(self):
        '''Counts the movies ratings.
        '''

        logger.info("Counting movie ratings.")
        movie_ID_with_ratings_RDD = self.ratings_RDD.map(
            lambda x: (x[1], x[2])).groupByKey()
        movie_ID_with_avg_ratings_RDD = movie_ID_with_ratings_RDD.map(
            rating_count_avg)
        self.movies_rating_counts_RDD = movie_ID_with_avg_ratings_RDD.map(
            lambda x: (x[0], x[1][0]))

    def train_recommender(self):
        '''Train the ALS model with the current dataset.
        '''

        logger.info("Training the ALS model.")
        self.model = ALS.train(self.ratings_RDD, self.rank, seed=self.seed,
                               iterations=self.iterations, lambda_=self.regularization_parameter)
        logger.info("ALS model built!")

    def predict_recommender(self, user_and_movie_RDD):
        '''Returns predictions for a given userID and movieID.
        '''

        predicted_RDD = self.model.predictAll(user_and_movie_RDD)
        predicted_rating_RDD = predicted_RDD.map(
            lambda x: (x.product, x.rating))
        predicted_rating_title_and_count_RDD = \
            predicted_rating_RDD.join(self.movies_titles_RDD).join(
                self.movies_rating_counts_RDD)
        predicted_rating_title_and_count_RDD = \
            predicted_rating_title_and_count_RDD.map(
                lambda r: (r[1][0][1], r[1][0][0], r[1][1]))

        return predicted_rating_title_and_count_RDD

    def add_new_user_ratings(self, ratings):
        '''Add movie ratings for the new user
        with user_id = 0
        input format = (user_id, movie_id, rating)
        '''

        # Convert ratings to an RDD
        new_ratings_RDD = self.sc.parallelize(ratings)
        # Add new ratings to the existing ones
        self.ratings_RDD = self.ratings_RDD.union(new_ratings_RDD)
        # Re-compute movie ratings count
        self.ratings_count_avg()
        # Re-train the ALS model with the new ratings
        self.train_recommender()

        return ratings

    def predict_top_ratings(self, user_id, movies_count):
        '''Recommends up to movies_count top unrated movies to user_id.
        '''

        # Get pairs of (userID, movieID) for user_id unrated movies
        user_unrated_movies_RDD = self.ratings_RDD.filter(lambda rating: not rating[0] == user_id)\
            .map(lambda x: (user_id, x[1])).distinct()
        # Get predicted ratings
        ratings = self.predict_recommender(user_unrated_movies_RDD).filter(
            lambda r: r[2] >= 25).takeOrdered(movies_count, key=lambda x: -x[1])
        return ratings

    def run(self, new_user_ratings):
        '''add and predict the unrated movies for 
        the new user.
        '''

        try:
            self.add_new_user_ratings(new_user_ratings)
            return self.predict_top_ratings(0, 37)

        except Exception as e:
            logger.exception(e)
            return -1


if __name__ == '__main__':

        try:
            dataset_path = input(
                'Enter dataset path: ')
            new_user_ratings = input(
                'Enter new user\'s rating in the format of (user_id, movie_id, rating): ')
            movieme = MovieMe(dataset_path)
            movieme.run(new_user_ratings)

        except Exception as e:
            logger.exception(e)
