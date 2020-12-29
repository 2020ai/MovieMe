# -*- encoding: utf-8 -*-
"""
MIT License
Copyright (c) 2019 - present AppSeed.us
"""

from app.home import blueprint, MovieMe
from flask import render_template, redirect, url_for, request
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
        movie_name_dict = {318:"Shawshank Redemption  The (1994)", 858:'Godfather  The (1972)' , 58559:'Dark Knight  The (2008)', 
        7153:'Lord of the Rings: The Return of the King  The (2003)', 527:'Schindler\'s List (1993)', 79132:'Inception (2010)',
        356:'Forrest Gump (1994)', 1196:'Star Wars: Episode V - The Empire Strikes Back (1980)', 2324:'Life Is Beautiful (La Vita è bella) (1997)',
        364:'Lion King  The (1994)', 5995:'Pianist  The (2002)', 2959:'Fight Club (1999)'}

        movie_id_dict = {'Movie1': 318, 'Movie2': 858, 'Movie3': 58559,
                         'Movie4': 7153, 'Movie5': 527, 'Movie6': 79132, 'Movie7': 356,
                         'Movie8': 1196, 'Movie9': 2324, 'Movie10': 364, 'Movie11': 5995, 'Movie12': 2959}
        # get the user's ratings
        user_ratings = [(0, int(movie_id_dict[rating]), int(request.form[rating]))
                        for rating in request.form if request.form[rating] != '']
        print('++++++++++user ratings:', user_ratings)
        # initiate the model
        movieme = MovieMe.MovieMe('dataset')
        rec_movies_unfiltered = movieme.run(user_ratings)
        REC_MOVIES = [x for x in rec_movies_unfiltered if x[0] not in [movie_name_dict[r[1]] for r in user_ratings]]
        print('++++++++++recommended movies: ', REC_MOVIES)
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
                           movie8=REC_MOVIES[7][0], rating8=round(REC_MOVIES[7][1], 2), total_rating8=REC_MOVIES[7][2],
                           movie9=REC_MOVIES[8][0], rating9=round(REC_MOVIES[8][1], 2), total_rating9=REC_MOVIES[8][2],
                           movie10=REC_MOVIES[9][0], rating10=round(REC_MOVIES[9][1], 2), total_rating10=REC_MOVIES[9][2],
                           movie11=REC_MOVIES[10][0], rating11=round(REC_MOVIES[10][1], 2), total_rating11=REC_MOVIES[10][2],
                           movie12=REC_MOVIES[11][0], rating12=round(REC_MOVIES[11][1], 2), total_rating12=REC_MOVIES[11][2],
                           movie13=REC_MOVIES[12][0], rating13=round(REC_MOVIES[12][1], 2), total_rating13=REC_MOVIES[12][2],
                           movie14=REC_MOVIES[13][0], rating14=round(REC_MOVIES[13][1], 2), total_rating14=REC_MOVIES[13][2],
                           movie15=REC_MOVIES[14][0], rating15=round(REC_MOVIES[14][1], 2), total_rating15=REC_MOVIES[14][2],
                           movie16=REC_MOVIES[15][0], rating16=round(REC_MOVIES[15][1], 2), total_rating16=REC_MOVIES[15][2],
                           movie17=REC_MOVIES[16][0], rating17=round(REC_MOVIES[16][1], 2), total_rating17=REC_MOVIES[16][2],
                           movie18=REC_MOVIES[17][0], rating18=round(REC_MOVIES[17][1], 2), total_rating18=REC_MOVIES[17][2],
                           movie19=REC_MOVIES[18][0], rating19=round(REC_MOVIES[18][1], 2), total_rating19=REC_MOVIES[18][2],
                           movie20=REC_MOVIES[19][0], rating20=round(REC_MOVIES[19][1], 2), total_rating20=REC_MOVIES[19][2],
                           movie21=REC_MOVIES[20][0], rating21=round(REC_MOVIES[20][1], 2), total_rating21=REC_MOVIES[20][2],
                           movie22=REC_MOVIES[21][0], rating22=round(REC_MOVIES[21][1], 2), total_rating22=REC_MOVIES[21][2],
                           movie23=REC_MOVIES[22][0], rating23=round(REC_MOVIES[22][1], 2), total_rating23=REC_MOVIES[22][2],
                           movie24=REC_MOVIES[23][0], rating24=round(REC_MOVIES[23][1], 2), total_rating24=REC_MOVIES[23][2],
                           movie25=REC_MOVIES[24][0], rating25=round(REC_MOVIES[24][1], 2), total_rating25=REC_MOVIES[24][2])

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
