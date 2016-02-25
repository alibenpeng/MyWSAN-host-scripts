#!/usr/bin/perl

use JSON;
use IO::Handle;
use IPC::Open3;
use threads;
use threads::shared;

use sigtrap qw/ handler signalHandler normal-signals error-signals /;

use strict;
use warnings;


my $serialdev = "/dev/ttyAMA0";
my $baudrate = "57600";

my $mqtt_server = "localhost";
my $mqtt_user = "rf12gw";
my $mqtt_pass = "sCHjd5%3m8XbSj";
#my $mqtt_auth = "-u '$mqtt_user' -P '$mqtt_pass'"
my $mqtt_auth = "";
my $mqtt_port = 1884;
my $mqtt_cacert = "/etc/mosquitto/ca_certificates/ca.crt";
#my $mqtt_tls = "--cafile $mqtt_cacert";
my $mqtt_tls = "";
my $mqtt_topic_prefix = "RF12";


sub startSocat {
    my $pkilled = 0;
    foreach (`ps ax | grep [s]ocat`) {
        chomp;
        if (m/^\s*(\d+)\s.*/) {
            print "killing socat PID '$1'\n";
            system "kill -9 $1";
            $pkilled = 1;
        }
    }

    if ($pkilled > 0) {
        sleep 1;
        print "killed remaining clients\n";
    }


    # open command pipe
    open3(\*SERIAL_IN, \*SERIAL_OUT, \*SERIAL_ERR, "socat $serialdev,raw,b$baudrate,echo=0 STDIO");
}

sub startMqttSub {
    open MQTT_SUB, "mosquitto_sub -I rf12_gateway $mqtt_tls -v -t '$mqtt_topic_prefix/#' -h $mqtt_server -p $mqtt_port $mqtt_auth --will-topic '$mqtt_topic_prefix/connected' --will-payload '0' |" or die "could not open mqtt client";
}

sub mqttPub {
    my ($topic, $payload, $flags) = @_;
    system "mosquitto_pub -I rf12_gateway $flags $mqtt_tls -h $mqtt_server -p $mqtt_port $mqtt_auth -t $mqtt_topic_prefix/$topic -m '$payload'";
}

sub signalHandler {
    my ($sig) = @_;
    print "GOT SIGNAL: $sig\n";
    if ($sig =~ /^PIPE$/) {
        warn "A pipe just broke...\n";
    } elsif ($sig =~ /^(KILL|INT|TERM)$/) {
        die "Killed by User!";
    }
}

################################
#     MQTT --> RF12 Drivers    #
################################

sub actionSmartmeter {
  my ($action, $data) = @_;
  print "not implemented!\n";
}

sub actionSensornode {
  my ($action, $data) = @_;
  my %commands = (
    "set" => 0,
  );
  my $command = $commands{$action};

  if ($data =~ m#(\d+) (\d+)$#) {
    my $id = $1;
    my $state = $3;

    if (defined($command) and ($command < 1)) {
      print "sending on serial port: $id,0,0,0,0,0,0,0,0,1,4a\n";
      print SERIAL_IN "$id,0,0,0,0,0,0,0,0,1,4a\n";

    }
  }
}


sub actionSocket {
  my ($action, $data) = @_;
  my %commands = (
    "query" => 0,
    "set" => 1,
    "toggle" => 2,
    "reset_ID", => 3,
    "set_new_ID", => 4,
    "request_new_ID", => 5,
    "bulk_query", => 6,
    "bulk_set", => 7,
    "heartbeat", => 8,
  );
  my $command = $commands{$action};

  if ($data =~ m#(\d+)/(\d+) (\d+)#) {
    my $id = $1;
    my $sub_id = $2;
    my $state = $3;

    if (($command) and ($command < 3)) {
      print "sending on serial port: $id,$sub_id,$command,$state,8s\n";
      print SERIAL_IN "$id,$sub_id,$command,$state,8s\n";
    }
  }
}

sub actionGateway {
  my ($action, $data) = @_;
  my %commands = (
    "connected" => 0,
    "acks" => 1,
  );
  my $command = $commands{$action};

  if ($data =~ m#(\d+) (\d+)#) {
    my $id = $1;
    my $payload = $2;

    if ($command) {
      if ($command == 0) {
        print SERIAL_IN "v\n";
      } elsif ($command == 1) {
        print SERIAL_IN $payload . "b\n";
      }
    }
  }
}

sub actionGateway {
}

sub mqttToRF12 {
  my $data = shift;
  if ($data =~ m#$mqtt_topic_prefix/(\w+)/(\w+)/(.*)#) {
    my $action = $1;
    my $dev_type = $2;
    my $payload = $3;
    if ($dev_type eq "smartmeter") {
      actionSmartmeter($action, $payload);
    } elsif ($dev_type eq "sensornode") {
      actionSensornode($action, $payload);
    } elsif ($dev_type eq "socket") {
      actionSocket($action, $payload);
    } elsif ($dev_type eq "gateway") {
      actionGateway($action, $payload);
    }
  }
}


################################
#     RF12 --> MQTT Drivers    #
################################

sub pubSmartmeter {
  my $payload = shift;
  if ($payload =~ /(\d+ \d+) (\d+ \d+) (\d+ \d+) (\d+ \d+) (\d+ \d+) (\d+ \d+)/) {
    my @counts = ($1, $3, $5);
    my @times = ($2, $4, $6);
    foreach (@counts, @times) {
      my ($highbyte, $lowbyte) = split(/ /, $_);
      $_ = sprintf("%02x%02x", $lowbyte, $highbyte);
      print "smartmeter value: $_ ";
      printf("%d\n", hex($_));
    }
  }
}

sub pubSensornode {
  my $payload = shift;

  if ($payload =~ /(\d+) (\d+) (\d+) (\d+) (\d+) (\d+) (\d+) (\d+) (\d+) (\d+)/) {

    #  1  0  0 241 255 189 0 1 0  0
    #  4 228 0 92  1 206 0 81  0  0
    # 04 a4 01 61 01 ce 00 32 00 00
    #  |  |  |   |     |     |    | 
    #  |  |  |   |     |     |     - Byte lobat/action 000000al
    #  |  |  |   |     |      ------ UInt16LE vsol
    #  |  |  |   |      ------------ UInt16LE vbat
    #  |  |  |    ------------------ Int16LE temp
    #  |  |   ---------------------- Byte moved/humi hhhhhhhm
    #  |   ------------------------- Byte light
    #   ---------------------------- Byte mcu_id


    my $id = $1;
    my $light = $2;
    my $moved = $3 & 0x01;
    my $humi = $3 >> 1;

    my $temp = hex(sprintf("%02x%02x", $5, $4));
    $temp = pack('s*', $temp);
    $temp = unpack('s*', $temp);
    $temp = (($temp * 6) + $temp / 4) * 10;
    my $vbat = int(hex(sprintf("%02x%02x", $7, $6)) * 3300 / 511);
    my $vsol = int(hex(sprintf("%02x%02x", $9, $8)) * 3300 / 511);
    my $lowbat = $10 & 0x01;
    my $action = ($10 >> 1) & 0x01;


    my %ret = (
      light => $light,
      moved => $moved,
      humi => $humi,
      temp => $temp/1000,
      vbat => $vbat/1000,
      vsol => $vsol/1000,
      lowbat => $lowbat,
      val => $action,
    );

    my $json_string = encode_json(\%ret);
    mqttPub("status/sensornode/$id", $json_string, "");
  }
}

sub pubSocket {
  my $payload = shift;
  my @socket_states = (
    "query", # 0
    "set", # 1
    "toggle", # 2
    "reset_ID", # 3
    "set_new_ID", # 4
    "request_new_ID", # 5
    "bulk_query", # 6
    "bulk_set", # 7
    "heartbeat", # 8
  );

  if ($payload =~ /(\d+) (\d+) (\d+) (\d+)/) {
    my $id = $1;
    my $sub_id = $2;
    my $command = $3;
    my $state = $4;
    if (($command < 3) || ($command > 5)) {
      if (($sub_id == 1) or ($command <= 2)) {
        mqttPub("status/socket/$id/$sub_id", $state, "-r");
      } else {
        foreach my $bit (1..$sub_id) {
          mqttPub("status/socket/$id/$bit", $state & 1, "-r");
          $state = $state >> 1;
        }
      }
    } else {
        mqttPub("$socket_states[$command]/socket/$id/$sub_id", $state, "-r");
    }
  }
}


sub pubButton {
  my $payload = shift;

  if ($payload =~ /(\d+) (\d+) (\d+) (\d+)/) {
    my $id = $1;
    my $button_id = $2;
    my $vbefore = $3;
    my $vafter = $4;
    mqttPub("status/button/$id/$button_id", 1, "");
  }
}


sub rf12ToMqtt {
  my $rf12 = shift;
  if ($rf12 =~ /RF12 (\d+) ([\d\s]+)/) {
    my $id = $1;
    my $payload = $2;
    if ($id == 3) {
      pubButton($payload);
    } elsif ($id == 4) {
      pubSensornode($payload);
    } elsif ($id == 5) {
      pubSmartmeter($payload);
    } elsif ($id == 8) {
      pubSocket($payload);
    }
  }
}

#
#
#


sub serialLoop {
    while (<SERIAL_OUT>) {
        print;
        rf12ToMqtt($_);
        chomp;
    }
}

sub stdinLoop {
    while (<STDIN>) {
        print SERIAL_IN $_;
    }
}

sub mqttLoop {
    while (<MQTT_SUB>) {
      print;
      mqttToRF12($_);
    }
}


# start the script..
print "[Ali's RFM12 <--> MQTT Gateway]\n";
print "startup...\n\n";

startSocat;
startMqttSub;

my @threads;
my $t = threads->new(\&serialLoop, 1);
push(@threads, $t);
$t = threads->new(\&stdinLoop, 2);
push(@threads, $t);
$t = threads->new(\&mqttLoop, 3);
push(@threads, $t);

foreach(@threads) {
    $_->join;
}

