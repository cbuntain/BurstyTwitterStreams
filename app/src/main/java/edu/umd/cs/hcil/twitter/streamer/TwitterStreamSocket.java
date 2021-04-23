package edu.umd.cs.hcil.twitter.streamer;

import twitter4j.*;
import twitter4j.Logger;

import java.io.IOException;
import java.util.logging.*;

/**
 * Created by cbuntain on 7/19/15.
 */
public class TwitterStreamSocket {

    private static final java.util.logging.Logger LOGGER =
            java.util.logging.Logger.getLogger(TwitterStreamSocket.class.getName());

    public static void createStreamSource(final String host, final int port)
    {
        LOGGER.log(Level.INFO, "Sending on socket: {0}:{1}", new Object[]{host, port});

        new Thread(new Runnable() {
            public void run() {
                try {
                    SocketSender sender = new SocketSender(host, port);

                    LOGGER.log(Level.INFO, "Waiting for connection...");
                    sender.accept();

                    createProducer(sender);
                } catch (IOException ioe) {
                    LOGGER.log(Level.SEVERE, "Failed to create socket...");
                    LOGGER.log(Level.SEVERE, ioe.getMessage());
                }
            }
        }).start();
    }

    public static void createProducer(final SocketSender s) throws IOException {

        StatusListener listener = new StatusListener() {
            public void onStatus(Status status) {
                if ( status.getLang().compareToIgnoreCase("en") == 0 ) {
                    String statusJson = TwitterObjectFactory.getRawJSON(status);
                    s.send(statusJson);
                }
            }

            public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {
//                System.out.println("Got a status deletion notice id:" + statusDeletionNotice.getStatusId());
            }

            public void onTrackLimitationNotice(int numberOfLimitedStatuses) {
//                System.out.println("Got track limitation notice:" + numberOfLimitedStatuses);
            }

            public void onScrubGeo(long userId, long upToStatusId) {
//                System.out.println("Got scrub_geo event userId:" + userId + " upToStatusId:" + upToStatusId);
            }

            public void onStallWarning(StallWarning warning) {
//                System.out.println("Got stall warning:" + warning);
            }

            public void onException(Exception ex) {
                ex.printStackTrace();
            }
        };

        TwitterStream twitterStream = new TwitterStreamFactory().getInstance();
        twitterStream.addListener(listener);
        twitterStream.sample();
    }

}
