package com.halorecsys;

import com.halorecsys.utils.Config;
import com.halorecsys.dataloader.DataLoader;

import java.net.URL;
import java.net.URI;
import java.net.InetSocketAddress;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.DefaultServlet;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.util.resource.Resource;

/**
 * @program: HaloRecSys
 * @description: this is the main application entry
 * @author: HaloZhang
 * @create: 2021-05-06 14:11
 **/
public class RecServer {
    public static void main(String[] args) throws Exception {
        new RecServer().run();
    }

    public void run() throws Exception {
        // halo recommendation system port number
        int port = Config.DEFAULT_PORT;
        try {
            port = Integer.parseInt(System.getenv("PORT"));
        } catch (NumberFormatException ignored) {
        }

        // set ip and port number
        InetSocketAddress inetAddress = new InetSocketAddress("0.0.0.0", port);
        Server server = new Server(inetAddress);

        // get index.html path
        URL webRootLocation = this.getClass().getResource("/webroot/index.html");
        if (webRootLocation == null) {
            throw new IllegalStateException("can't determine webroot URL location");
        }

        URI webRootUri = URI.create(webRootLocation.toURI().toASCIIString().replaceFirst("/index.html$","/"));
        System.out.printf("Web Root URI: %s%n", webRootUri.getPath());

        DataLoader.getInstance().LoadMovieData(Config.MONGODB_RECOMMENDATION_DB, Config.MONGODB_MOVIE_COLLECTION,
                Config.MONGODB_RATING_COLLECTION, Config.MONGODB_LINK_COLLECTION);
        DataLoader.getInstance().LoadStatisticsRecsData(Config.MONGODB_RECOMMENDATION_DB, Config.RATE_MOST_MOVIES,
                Config.RATE_MOST_RECENTLY_MOVIES, Config.AVERAGE_RATINGS_MOVIES, Config.GENRES_TOP_N_MOVIES);
        DataLoader.getInstance().LoadLFMRecsData(Config.LFM_MOVIE_RECS, Config.LFM_USER_RECS, Config.LFM_USER_SIM_RECS);

        // create server context
        ServletContextHandler context = new ServletContextHandler();
        context.setContextPath("/");
        context.setBaseResource(Resource.newResource(webRootUri));
        context.setWelcomeFiles(new String[] { "index.html" });
        context.getMimeTypes().addMimeMapping("txt","text/plain;charset=utf-8");

        // register different service to servlets
        context.addServlet(DefaultServlet.class,"/");
//        context.addServlet(new ServletHolder(new MovieService()), "/getmovie");
//        context.addServlet(new ServletHolder(new UserService()), "/getuser");
//        context.addServlet(new ServletHolder(new SimilarMovieService()), "/getsimilarmovie");
//        context.addServlet(new ServletHolder(new RecommendationService()), "/getrecommendation");
//        context.addServlet(new ServletHolder(new RecForYouService()), "/getrecforyou");

        server.setHandler(context);
        System.out.println("Halo recommendation system has started");

        server.start();
        server.join();
    }
}
