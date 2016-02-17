#!/usr/bin/perl

use strict;
use warnings;

my $map_file = "./topic_map.csv";

my $mqtt_server = "localhost";
my $mqtt_user = "topic_mapper";
my $mqtt_pass = "sARy<F5tE77vXwH";
my $mqtt_port = 1884;
my $mqtt_cacert = "/etc/mosquitto/ca_certificates/ca.crt";
#my $mqtt_tls = "--cafile $mqtt_cacert";
my $mqtt_tls = "";

use vars qw/ @processed_messages @topics $new_topic /;


sub startMqttSub {
    open MQTT_SUB, "mosquitto_sub -I rf12_gateway $mqtt_tls -v -t '#' -h $mqtt_server -p $mqtt_port -u $mqtt_user -P '$mqtt_pass' |" or die "could not open mqtt client";
}

sub mqttPub {
    my ($topic, $payload) = @_;
    system "mosquitto_pub -r -I rf12_gateway $mqtt_tls -h $mqtt_server -p $mqtt_port -u $mqtt_user -P '$mqtt_pass' -t '$topic' -m '$payload'";
}

sub swapTopics {
  my $msg = shift;
  chomp($msg);

  if (grep(/$msg/, @processed_messages)) {
    print "already processed: $msg\n";
    my @tmp = grep(!/$msg/, @processed_messages);
    @processed_messages = @tmp;
  print "array length: " . $#processed_messages . "\n";
    return;
  }

  if ($msg =~ m/([^\s]+)\s(.*)/) {
    my ($topic, $payload) = ($1, $2);
    #print "topic: $topic, payload: $payload\n"

    my $new_topic;

    if ($topic =~ m#^raw/#) {
      foreach(@topics){
        if ($topic =~ m/$_->[0](.*)/) {
          $new_topic = $_->[1] . $1;
          last;
        }
      }
    } else {
      foreach(@topics){
        if ($topic =~ m/$_->[1](.*)/) {
          $new_topic = $_->[0] . $1;
          last;
        }
      }
    }

    if ($new_topic) {
      print "$topic -> $new_topic\n";
      mqttPub($new_topic, $payload);
      push(@processed_messages, "$new_topic $payload");
    } else {
      print "Unknown Device: $topic\n";
    }
  }
  print "array length: " . $#processed_messages . "\n";
}

open MAP, "<$map_file" or die "can not open map file!";

foreach (<MAP>) {
  chomp();
  if (m/([^,]+),([^,]+)/) {
    print "adding topic pair: $1 $2\n";
    push(@topics, [$1, $2]);
  }
}

startMqttSub();

while (<MQTT_SUB>) {
  swapTopics($_);
}
