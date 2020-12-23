# -*- encoding: utf-8 -*-
"""
MIT License
Copyright (c) 2019 - present AppSeed.us
"""

from app.home import blueprint, MovieFy
from flask import render_template, redirect, url_for, request
from flask_login import login_required, current_user
from app import login_manager
from jinja2 import TemplateNotFound

@blueprint.route('/')
def route_default():
    return redirect(url_for('home_blueprint.getRatings'))


@blueprint.route('/getRatings', methods=['GET', 'POST'])
def getRatings():
    global REC_MOVIES

    if request.method == 'GET':
        return render_template('login.html')
    elif request.method == 'POST':
        # a dictionary of movies' ID in the dataset
        movie_id_dict = {'Movie1': 260, 'Movie2': 1, 'Movie3': 25, 
        'Movie4': 296, 'Movie5': 585, 'Movie6': 50, 'Movie7': 16, 
        'Movie8': 32, 'Movie9': 335, 'Movie10': 379}
        # get the user's ratings
        user_ratings = [(0, int(movie_id_dict[rating]), int(request.form[rating]))
                        for rating in request.form if request.form[rating] != '']
        print('++++++++++user ratings:', user_ratings)
        # initiate the model
        moviefy = MovieFy.MovieFy('ml-latest-small')
        REC_MOVIES = moviefy.run(user_ratings)
        print('------', REC_MOVIES)
        # user_ratings = [
        #     (0, 260, 4),  # Star Wars (1977)
        #     (0, 1, 3),  # Toy Story (1995)
        #     (0, 25, 4),  # Leaving Las Vegas (1995)
        #     (0, 296, 3),  # Pulp Fiction (1994)
        #     (0, 858, 5),  # Godfather, The (1972)
        #     (0, 50, 4)  # Usual Suspects, The (1995)
        #     (0, 16, 3),  # Casino (1995)
        #     (0, 32, 4),  # Twelve Monkeys (a.k.a. 12 Monkeys) (1995)
        #     (0, 335, 1),  # Flintstones, The (1994)
        #     (0, 379, 1),  # Timecop (1994)
        # ]
        # movie1_rating = request.form['Movie1']
        return redirect(url_for('home_blueprint.index'))
    else:
        return render_template('login.html')


@blueprint.route('/index')
def index():

    return render_template('index.html',
                           movie1=REC_MOVIES[0][0], rating1=round(REC_MOVIES[0][1],2), total_rating1=REC_MOVIES[0][2],
                           movie2=REC_MOVIES[1][0], rating2=round(REC_MOVIES[1][1],2), total_rating2=REC_MOVIES[1][2],
                           movie3=REC_MOVIES[2][0], rating3=round(REC_MOVIES[2][1],2), total_rating3=REC_MOVIES[2][2],
                           movie4=REC_MOVIES[3][0], rating4=round(REC_MOVIES[3][1],2), total_rating4=REC_MOVIES[3][2],
                           movie5=REC_MOVIES[4][0], rating5=round(REC_MOVIES[4][1],2), total_rating5=REC_MOVIES[4][2],
                           movie6=REC_MOVIES[5][0], rating6=round(REC_MOVIES[5][1],2), total_rating6=REC_MOVIES[5][2],
                           movie7=REC_MOVIES[6][0], rating7=round(REC_MOVIES[6][1],2), total_rating7=REC_MOVIES[6][2],
                           movie8=REC_MOVIES[7][0], rating8=round(REC_MOVIES[7][1], 2), total_rating8=REC_MOVIES[7][2],)

@blueprint.route('/<template>')
def route_template(template):

    try:

        if not template.endswith( '.html' ):
            template += '.html'

        return render_template( template )

    except TemplateNotFound:
        return render_template('page-404.html'), 404
    
    except:
        return render_template('page-500.html'), 500
