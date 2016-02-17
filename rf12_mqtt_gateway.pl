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
my $mqtt_port = 1884;
my $mqtt_cacert = "/etc/mosquitto/ca_certificates/ca.crt";
#my $mqtt_tls = "--cafile $mqtt_cacert";
my $mqtt_tls = "";
my $mqtt_topic_prefix = "raw/RF12";

my %device_types = (
    "4" => "balcony",
    "5" => "smartmeter",
    "8" => "socket",
);

my @pstates = (
    "off",
    "on",
);

sub parsePayload {
    my $p = $_[0];
    print "payload parsed\n";
}


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
    open MQTT_SUB, "mosquitto_sub -I rf12_gateway $mqtt_tls -v -t '$mqtt_topic_prefix/#' -h $mqtt_server -p $mqtt_port -u $mqtt_user -P $mqtt_pass |" or die "could not open mqtt client";
}

sub mqttPub {
    my ($topic, $payload) = @_;
    system "mosquitto_pub -I rf12_gateway -r $mqtt_tls -h $mqtt_server -p $mqtt_port -u $mqtt_user -P $mqtt_pass -t $mqtt_topic_prefix/$topic -m '$payload'";
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
  my $data = shift;
  print "not implemented!\n";
}

sub actionSensornode {
  my $data = shift;
  my %commands = (
    "set" => 0,
  );
  if ($data =~ m#(\d+)/(\w+) (\d+)$#) {
    my $id = $1;
    my $command = $commands{$2};
    my $state = $3;

    if (defined($command) and ($command < 1)) {
      print "sending on serial port: $id,0,0,0,0,0,0,0,0,1,4a\n";
      print SERIAL_IN "$id,0,0,0,0,0,0,0,0,1,4a\n";

    }
  }
}

sub actionSocket {
  my $data = shift;
  my %commands = (
    "query" => 0,
    "set" => 1,
    "toggle" => 2
  );
  if ($data =~ m#(\d+)/(\d+)/(\w+) (\d+)#) {
    my $id = $1;
    my $sub_id = $2;
    my $command = $commands{$3};
    my $state = $4;

    if (($command) and ($command < 3)) {
      print "sending on serial port: $id,$sub_id,$command,$state,8s\n";
      print SERIAL_IN "$id,$sub_id,$command,$state,8s\n";
    }
  }
}

sub actionGateway {
  my $data = shift;
  my %commands = (
    "acks" => 0,
  );
  if ($data =~ m#(\d+)/(\d+)/(\w+) (\d+)#) {
    my $id = $1;
    my $sub_id = $2;
    my $command = $commands{$3};
    my $state = $4;

    if (($command) and ($command < 1)) {
      print "sending on serial port: $id,$sub_id,$command,$state,8s\n";
      print SERIAL_IN "$id,$sub_id,$command,$state,8s\n";
    }
  }
}

sub mqttToRF12 {
  my $data = shift;
  if ($data =~ m#$mqtt_topic_prefix/(\w+)/(.*)#) {
    my $dev_type = $1;
    my $payload = $2;
    if ($dev_type eq "smartmeter") {
      actionSmartmeter($payload);
    } elsif ($dev_type eq "sensornode") {
      actionSensornode($payload);
    } elsif ($dev_type eq "socket") {
      actionSocket($payload);
    } elsif ($dev_type eq "gateway") {
      actionGateway($payload);
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
      action => $action,
    );

    my $json_string = encode_json(\%ret);
    mqttPub("sensornode/$id/data", $json_string);
  }
}

sub pubSocket {
  my $payload = shift;
  my @socket_states = (
    "state", # query
    "state", # set
    "state", # toggle
    3,
    4,
    "req_id",
    6,
    7,
    "state",
    "req_state",
  );

  if ($payload =~ /(\d+) (\d+) (\d+) (\d+)/) {
    my $id = $1;
    my $sub_id = $2;
    my $command = $3;
    my $state = $4;
    if (($sub_id == 1) or ($command <= 2)) {
      mqttPub("socket/$id/$sub_id/$socket_states[$command]", $state);
    } else {
      foreach my $bit (1..$sub_id) {
        mqttPub("socket/$id/$bit/$socket_states[$command]", $state & 1);
        $state = $state >> 1;
      }
    }
  }
}


sub rf12ToMqtt {
  my $rf12 = shift;
  if ($rf12 =~ /RF12 (\d+) ([\d\s]+)/) {
    my $id = $1;
    my $payload = $2;
    if ($id == 4) {
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

