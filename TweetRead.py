import tweepy
from tweepy import OAuthHandler
from tweepy import Stream
from tweepy.streaming import StreamListener
import socket
import json


# Set up your credentials

#consumer_key = "SOOo6EmsqAbfVMEidXy12DvRZ"
#consumer_secret = "Ni1OIqqkei0aq60vC8wrei2WPTpCCX4j0EXEBd80PPebOgUZKk"
#access_token = "404118511-z4rf7f1Qm85oWQncf7Y59yc1oKHQjhRFOdRhN2Wm"
#access_secret = "QqUSWaJr7GCak1P75PheBstQJjbZyrRZSRfzqFMyjvjEP"

consumer_key = "q9ixYqKnDF4tVCxnaoUV00fob"
consumer_secret = "6704Jv2hJkfefcdhFz5kG2vj45KOxqwrNhqzsqO4bJMEglojuY"
access_token = "2178510532-KmFZHsqOe2mSIfm1gRyAKrHMrSUkVzUf1vH2YrN"
access_secret = "zH0ZM61EOcfvftcaUN4qKcJiAgs1fSoHqpPWYlomMcyJI"

class TweetsListener(StreamListener):

  def __init__(self, csocket):
      self.client_socket = csocket

  def on_data(self, data):
      try:
          msg = json.loads( data )
          print( msg['text'].encode('utf-8') )
          self.client_socket.send( msg['text'].encode('utf-8') )
          return True
      except BaseException as e:
          print("Error on_data: %s" % str(e))
      return True

  def on_error(self, status):
      print(status)
      return True

def sendData(c_socket):
  auth = OAuthHandler(consumer_key, consumer_secret)
  auth.set_access_token(access_token, access_secret)

  twitter_stream = Stream(auth, TweetsListener(c_socket))
  twitter_stream.filter(track=['Donald Trump'])

if __name__ == "__main__":
  s = socket.socket()         # Create a socket object
  host = "127.0.0.1"     # Get local machine name
  port = 6789                # Reserve a port for your service.
  s.bind((host, port))        # Bind to the port

  print("Listening on port: %s" % str(port))

  s.listen(5)                 # Now wait for client connection.
  c, addr = s.accept()        # Establish connection with client.

  print( "Received request from: " + str( addr ) )

  sendData( c )
