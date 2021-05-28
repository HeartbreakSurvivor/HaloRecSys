
 function appendMovie2Row(rowId, movieName, movieId, year, rating, rateNumber, genres, baseUrl) {

    var genresStr = "";
    $.each(genres, function(i, genre){
        genresStr += ('<div class="genre"><a href="'+baseUrl+'collection.html?type=genre&value='+genre+'"><b>'+genre+'</b></a></div>');
    });


    var divstr = '<div class="movie-row-item" style="margin-right:5px">\
                    <movie-card-smart>\
                     <movie-card-md1>\
                      <div class="movie-card-md1">\
                       <div class="card">\
                        <link-or-emit>\
                         <a uisref="base.movie" href="./movie.html?movieId='+movieId+'">\
                         <span>\
                           <div class="poster">\
                            <img src="./posters/' + movieId + '.jpg" />\
                           </div>\
                           </span>\
                           </a>\
                        </link-or-emit>\
                        <div class="overlay">\
                         <div class="above-fold">\
                          <link-or-emit>\
                           <a uisref="base.movie" href="./movie.html?movieId='+movieId+'">\
                           <span><p class="title">' + movieName + '</p></span></a>\
                          </link-or-emit>\
                          <div class="rating-indicator">\
                           <ml4-rating-or-prediction>\
                            <div class="rating-or-prediction predicted">\
                             <svg xmlns:xlink="http://www.w3.org/1999/xlink" class="star-icon" height="14px" version="1.1" viewbox="0 0 14 14" width="14px" xmlns="http://www.w3.org/2000/svg">\
                              <defs></defs>\
                              <polygon fill-rule="evenodd" points="13.7714286 5.4939887 9.22142857 4.89188383 7.27142857 0.790044361 5.32142857 4.89188383 0.771428571 5.4939887 4.11428571 8.56096041 3.25071429 13.0202996 7.27142857 10.8282616 11.2921429 13.0202996 10.4285714 8.56096041" stroke="none"></polygon>\
                             </svg>\
                             <div class="rating-value">\
                              '+rating+'\
                             </div>\
                            </div>\
                           </ml4-rating-or-prediction>\
                          </div>\
                          <p class="year">'+year+'</p>\
                         </div>\
                         <div class="below-fold">\
                          <div class="genre-list">\
                           '+genresStr+'\
                          </div>\
                          <div class="ratings-display">\
                           <div class="rating-average">\
                            <span class="rating-large">'+rating+'</span>\
                            <span class="rating-total">/5</span>\
                            <p class="rating-caption"> '+rateNumber+' ratings </p>\
                           </div>\
                          </div>\
                         </div>\
                        </div>\
                       </div>\
                      </div>\
                     </movie-card-md1>\
                    </movie-card-smart>\
                   </div>';
    $('#'+rowId).append(divstr);
};

function addRowSplit(pageId, rowName) {
 var divstr = '<h2>' +rowName+ '</h2> \
        <hr style="color:#987cb9 size:3 !important">'
     $(pageId).prepend(divstr);
};

function addRowFrame(pageId, rowName, rowId, baseUrl) {
 var divstr = '<div class="frontpage-section-top"> \
                <div class="explore-header frontpage-section-header">\
                 <a class="plainlink" title="go to the full list" href="'+baseUrl+'collection.html?type=genre&value='+rowName+'">' + rowName + '</a> \
                </div>\
                <div class="movie-row">\
                 <div class="movie-row-bounds">\
                  <div class="movie-row-scrollable" id="' + rowId +'" style="margin-left: 0px;">\
                  </div>\
                 </div>\
                 <div class="clearfix"></div>\
                </div>\
               </div>'
     $(pageId).prepend(divstr);
};

function addRowFrameWithoutLink(pageId, rowName, rowId, baseUrl) {
 var divstr = '<div class="frontpage-section-top"> \
                <div class="explore-header frontpage-section-header">\
                  <span class="plainlink" >'+ rowName +' </span> \
                </div>\
                <div class="movie-row">\
                 <div class="movie-row-bounds">\
                  <div class="movie-row-scrollable" id="' + rowId +'" style="margin-left: 0px;">\
                  </div>\
                 </div>\
                 <div class="clearfix"></div>\
                </div>\
               </div>'
     $(pageId).prepend(divstr);
};

function addStreamingRecRow(pageId, rowName, rowId, username, size, baseUrl) {
    addRowFrame(pageId, rowName, rowId, baseUrl);
    $.getJSON(baseUrl + "getstreamingrec?username="+username+"&size="+size+"&sortby=rating", function(result) {
        $.each(result, function(i, movie){
          appendMovie2Row(rowId, movie.title, movie.movieId, movie.releaseYear, movie.averageRating.toPrecision(2), movie.ratingNumber, movie.genres,baseUrl);
        });
    });
};

function addGenreRow(pageId, rowName, rowId, size, baseUrl, type) {
    if (type == 0) {
        addRowFrame(pageId, rowName, rowId, baseUrl);
    }
    else if (type == 1) {
        addRowFrameWithoutLink(pageId, rowName, rowId, baseUrl);
    }
    $.getJSON(baseUrl + "getrecommendation?genre="+rowName+"&size="+size+"&sortby=rating"+"&type="+type, function(result){
        $.each(result, function(i, movie){
          appendMovie2Row(rowId, movie.title, movie.movieId, movie.releaseYear, movie.averageRating.toPrecision(2), movie.ratingNumber, movie.genres,baseUrl);
        });
    });
};

function addRelatedMovies(pageId, containerId, movieId, baseUrl){

    var rowDiv = '<div class="frontpage-section-top"> \
                <div class="explore-header frontpage-section-header">\
                 Related Movies \
                </div>\
                <div class="movie-row">\
                 <div class="movie-row-bounds">\
                  <div class="movie-row-scrollable" id="' + containerId +'" style="margin-left: 0px;">\
                  </div>\
                 </div>\
                 <div class="clearfix"></div>\
                </div>\
               </div>'
    $(pageId).prepend(rowDiv);

    $.getJSON(baseUrl + "getsimilarmovie?movieId="+movieId+"&size=16&mode=lfm", function(result){
            $.each(result, function(i, movie){
              appendMovie2Row(containerId, movie.title, movie.movieId, movie.releaseYear, movie.averageRating.toPrecision(2), movie.ratingNumber, movie.genres,baseUrl);
            });
    });
}

function addUserHistory(pageId, containerId, userId, baseUrl){

    var rowDiv = '<div class="frontpage-section-top"> \
                <div class="explore-header frontpage-section-header">\
                 User Watched Movies \
                </div>\
                <div class="movie-row">\
                 <div class="movie-row-bounds">\
                  <div class="movie-row-scrollable" id="' + containerId +'" style="margin-left: 0px;">\
                  </div>\
                 </div>\
                 <div class="clearfix"></div>\
                </div>\
               </div>'
    $(pageId).prepend(rowDiv);
    console.log("get user history...")
    $.getJSON(baseUrl + "getuser?id="+userId, function(userObject){
            $.each(userObject.ratings, function(i, rating){
                $.getJSON(baseUrl + "getmovie?id="+rating.rating.movieId, function(movieObject){
                    appendMovie2Row(containerId, movieObject.title, movieObject.movieId, movieObject.releaseYear, rating.rating.score, movieObject.ratingNumber, movieObject.genres, baseUrl);
                });
            });
    });
}

function addRecForYou(pageId, containerId, userId, baseUrl){

    var rowDiv = '<div class="frontpage-section-top"> \
                <div class="explore-header frontpage-section-header">\
                 Recommended For You \
                </div>\
                <div class="movie-row">\
                 <div class="movie-row-bounds">\
                  <div class="movie-row-scrollable" id="' + containerId +'" style="margin-left: 0px;">\
                  </div>\
                 </div>\
                 <div class="clearfix"></div>\
                </div>\
               </div>'
    $(pageId).prepend(rowDiv);

    $.getJSON(baseUrl + "getrecforyou?username="+userId+"&size=10&mode=lfm", function(result){
                $.each(result, function(i, movie){
                  appendMovie2Row(containerId, movie.title, movie.movieId, movie.releaseYear, movie.averageRating.toPrecision(2), movie.ratingNumber, movie.genres,baseUrl);
                });
     });
}


function addMovieDetails(containerId, movieId, baseUrl) {

    $.getJSON(baseUrl + "getmovie?id="+movieId, function(movieObject){
        var genres = "";
        $.each(movieObject.genres, function(i, genre){
            genres += ('<span><a href="'+baseUrl+'collection.html?type=genre&value='+genre+'"><b>'+genre+'</b></a>');
            if(i < movieObject.genres.length-1){
                genres+=", </span>";
            }else{
                genres+="</span>";
            }
        });

        var ratingUsers = "";
        $.each(movieObject.topRatings, function(i, rating){
            //console.log(rating);
            ratingUsers += ('<span><a href="'+baseUrl+'user.html?id='+rating.rating.userName+'"><b>'+rating.rating.userName+'</b></a>');
            if(i < movieObject.topRatings.length-1){
                ratingUsers+=", </span>";
            }else{
                ratingUsers+="</span>";
            }
        });
        //console.log("ratingUsers: ", ratingUsers);

        var movieDetails = '<div class="row movie-details-header movie-details-block">\
                                        <div class="col-md-2 header-backdrop">\
                                            <img alt="movie backdrop image" height="250" src="./posters/'+movieObject.movieId+'.jpg">\
                                        </div>\
                                        <div class="col-md-9"><h1 class="movie-title"> '+movieObject.title+' </h1>\
                                            <div class="row movie-highlights">\
                                                <div class="col-md-2">\
                                                    <div class="heading-and-data">\
                                                        <div class="movie-details-heading">Release Year</div>\
                                                        <div> '+movieObject.releaseYear+' </div>\
                                                    </div>\
                                                    <div class="heading-and-data">\
                                                        <div class="movie-details-heading">Links</div>\
                                                        <a target="_blank" href="http://www.imdb.com/title/tt'+movieObject.imdbId+'">imdb</a>,\
                                                        <span><a target="_blank" href="http://www.themoviedb.org/movie/'+movieObject.tmdbId+'">tmdb</a></span>\
                                                    </div>\
                                                    <div id="rate" class="heading-and-data">\
                                                        <div class="movie-details-heading">User Rating</div>\
                                                        <div id="user_rating" class="my-rating"></div>\
                                                    </div>\
                                                </div>\
                                                <div class="col-md-3">\
                                                    <div class="heading-and-data">\
                                                        <div class="movie-details-heading"> MovieLens predicts for you</div>\
                                                        <div> 5.0 stars</div>\
                                                    </div>\
                                                    <div class="heading-and-data">\
                                                        <div class="movie-details-heading"> Average of '+movieObject.ratingNumber+' ratings</div>\
                                                        <div> '+movieObject.averageRating.toPrecision(2)+' stars\
                                                        </div>\
                                                    </div>\
                                                </div>\
                                                <div class="col-md-6">\
                                                    <div class="heading-and-data">\
                                                        <div class="movie-details-heading">Genres</div>\
                                                        '+genres+'\
                                                    </div>\
                                                    <div class="heading-and-data">\
                                                        <div class="movie-details-heading">Who likes the movie most</div>\
                                                        '+ratingUsers+'\
                                                    </div>\
                                                </div>\
                                            </div>\
                                        </div>\
                                    </div>'
        $("#"+containerId).prepend(movieDetails);

        var userName = localStorage.getItem("userName");
        if (null != userName) {
            $('#rate').show();
            // set configuration for rating star
            $('#user_rating').starRating({
              initialRating: 0,
              strokeColor: '#FFA500',
              strokeWidth: 10,
              starSize: 25,
              ratedColors: '#888888',
              callback: function(currentRating, $el){
                    //alert('rated ' + currentRating);
                    console.log('DOM element ', $el);

                    var userName = localStorage.getItem("userName");
                    console.log(userName);
                    var date = new Date();
                    var timestamp = Math.round(date.getTime()/1000);

                    console.log('user | movie | score | timestamp:');
                    console.log(userName, movieId, currentRating, timestamp);

                    var getUrl = window.location;
                    var baseUrl = getUrl.protocol + "//" + getUrl.host + "/"
                    var url = baseUrl + "rating?username="+userName+"&movieId="+movieId+"&rating="+currentRating+"&timestamp="+timestamp
                    console.log(url)

                    $.ajax({
                        type : "POST",
                        async : false,
                        url : url,
                        dataType: "json",
                        timeout : 1000,
                        success : function(data) {
                            console.log(data);
                            //alert(data.msg);
                        },
                        error : function(errorMsg) {
                            alert(errorMsg)
                        }
                    });
              }
            });
        } else {
            console.log("user not login");
            $('#rate').hide();
        }

    });
};

function addUserDetails(containerId, userId, baseUrl) {
    $.getJSON(baseUrl + "getuser?id="+userId, function(userObject) {
        console.log("get similar users");
        var similarUsers = "";
        $.getJSON(baseUrl + "getsimilaruser?userName="+userId+"&size=10&mode=lfm", function(result){
               if (null !== result) {
                    console.log("result.length: ", result.length);
                      $.each(result, function(i, user) {
                              console.log("get users: ", user);
                              similarUsers += ('<span><a href="'+baseUrl+'user.html?id='+user.userName+'"><b>'+user.userName+'</b></a>');
                              if(i < result.length-1) {
                                  similarUsers+=", </span>";
                              }else{
                                  similarUsers+="</span>";
                              }
                      });
                     console.log("similarUsers: ", similarUsers);
               }

                var genres = "";
                console.log("userobject: ", userObject.prefGenres);

                $.each(userObject.prefGenres, function(i, g){
                        console.log("get genre: ", g);
                        genres += ('<span><a href="'+baseUrl+'collection.html?type=genre&value='+g+'"><b>'+g+'</b></a>');
                        if(i < userObject.prefGenres.length-1){
                            genres+=", </span>";
                        }else{
                            genres+="</span>";
                        }
                });
                console.log("genres: ", genres);

               var userDetails = '<div class="row movie-details-header movie-details-block">\
                           <div class="col-md-2 header-backdrop">\
                               <img alt="movie backdrop image" height="200" src="./images/avatar/'+Math.abs(userObject.userId)%10+'.png">\
                           </div>\
                           <div class="col-md-9"><h1 class="movie-title"> '+userObject.userName+' </h1>\
                               <div class="row movie-highlights">\
                                   <div class="col-md-2">\
                                       <div class="heading-and-data">\
                                           <div class="movie-details-heading">#Watched Movies</div>\
                                           <div> '+userObject.ratingCount+' </div>\
                                       </div>\
                                       <div class="heading-and-data">\
                                           <div class="movie-details-heading"> Average Rating Score</div>\
                                           <div> '+userObject.averageRating.toPrecision(2)+' stars\
                                           </div>\
                                       </div>\
                                   </div>\
                                   <div class="col-md-3">\
                                       <div class="heading-and-data">\
                                           <div class="movie-details-heading"> Highest Rating Score</div>\
                                           <div> '+userObject.highestRating+' stars</div>\
                                       </div>\
                                       <div class="heading-and-data">\
                                           <div class="movie-details-heading"> Lowest Rating Score</div>\
                                           <div> '+userObject.lowestRating+' stars\
                                           </div>\
                                       </div>\
                                   </div>\
                                   <div class="col-md-6">\
                                       <div class="heading-and-data">\
                                           <div class="movie-details-heading">Favourite Genres</div>\
                                           '+genres+'\
                                       </div>\
                                       <div class="heading-and-data">\
                                           <div class="movie-details-heading">Similar users</div>\
                                           '+similarUsers+'\
                                       </div>\
                                   </div>\
                               </div>\
                           </div>\
                       </div>'
            $("#"+containerId).prepend(userDetails);
        });
    });
};





