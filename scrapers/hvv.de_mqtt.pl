#!/usr/bin/perl

use IO::Handle;
use IPC::Open3;
use threads;
use threads::shared;

use sigtrap qw/ handler signalHandler normal-signals error-signals /;

use strict;
use warnings;

use JSON;
use Text::Iconv;


my $mqtt_broker = "localhost";
my $mqtt_port = "1884";
my $mqtt_user = 'scraper-hvv';
my $mqtt_pass = 'sLKkfC$66w2';
my $mqtt_cacert = "/etc/mosquitto/ca_certificates/ca.crt";
#my $mqtt_tls = "--cafile $mqtt_cacert";
my $mqtt_tls = "";
my $utf8_to_western = Text::Iconv->new("UTF-8", "WINDOWS-1252");
my $western_to_utf8 = Text::Iconv->new("ISO8859-1", "UTF-8");

#sub encode_json {
#  my $hashref = shift;
#  my @ret;
#  foreach(keys(%$hashref)) {
#    push(@ret, "\"" . $_ . "\":\"" . $hashref->{$_} . "\"");
#  }
#  return "{" . join(',', @ret) . "}";
#}


sub getSchedule {
  #my ($mqtt_broker, $station_name, $offset_minutes, $max_results) = @ARGV;
  my $station_ref = decode_json($_[0]);

  my $station_urlenc = $utf8_to_western->convert($station_ref->{name});
  $station_urlenc =~ s/([^^A-Za-z0-9\-_.!~*'()])/ sprintf "%%%0x", ord $1 /eg;

  my $site = "http://www.hvv.de";
  my $link = "fahrplaene/abfahrtsmonitor/index.php";

  my (undef, $min, $hour, $mday, $mon, $year) = localtime(time + ($station_ref->{time_offset} * 60));
  my $date = sprintf("%02d.%02d.%04d", $mday, $mon + 1, $year + 1900);
  my $time = sprintf("%02d%%3A%02d", $hour, $min);

  # station=H%F6genstra%DFe&on=03.02.2016&at=01%3A09&timerefreshvalue=1%3A13&timerefreshbtn=refresh&listing_field=&transport_type%5B%5D=BUS&transport_type%5B%5D=TRAIN&transport_type%5B%5D=SHIP
  my $post_data = "station=$station_urlenc&on=$date&at=$time&timerefreshvalue=&listing_field=&transport_type%5B%5D=BUS&transport_type%5B%5D=TRAIN&transport_type%5B%5D=SHIP";


  my @http_headers = (
    "Host: www.hvv.de",
    "User-Agent: Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:44.0) Gecko/20100101 Firefox/44.0",
    "Accept: text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language: en-US,en;q=0.5",
    "Accept-Encoding: gzip, deflate",
    "Referer: http://www.hvv.de/fahrplaene/abfahrtsmonitor/index.php",
    "Cookie: hvv_sso=82f555d91ffeb8d5414d1a8442620962",
    "Connection: keep-alive",
  );

  my $header;
  foreach (@http_headers) {
    $header .= "--header='" . $_ . "' ";
  }

  my @page = `wget $site/$link $header --post-data='$post_data' -O -`;

  my $in_row = 0;
  my @rows;
  my $row_counter = 0;

  foreach (@page) {
    if (m#<tr class="(odd|even)">#s) {
      $in_row = 1;
    }

    if ($in_row == 1) {
      chomp();
      $rows[$row_counter] .= $_;
      if (m#</tr>#) {
        $rows[$row_counter] .= "\n";
        $in_row = 0;
        $row_counter++;
      }
    }
  }

  print "\nparsing rows\n\n";
  my $result_counter = 0;
  foreach (@rows) {
    #print "ROW: $_";

  #ROW:                    <tr class="odd">                        <td align="center"><img src="http://www.geofox.de/icon_service/line?height=14&lineKey=ZVU-DB:S21_ZVU-DB_S-ZVU" /></td>      <td>Aum�hle</td>                          <td>                         06:12                                                   </td>           </tr>
  #ROW:                    <tr class="even">                       <td align="center"><img src="http://www.geofox.de/icon_service/line?height=14&lineKey=ZVU-DB:S3_ZVU-DB_S-ZVU" /></td>      <td>Pinneberg</td>                         <td>                         06:14                                                   </td>           </tr>

  #ROW:                    <tr class="even">                       <td align="center"><img src="http://www.geofox.de/icon_service/line?height=14&lineKey=ZVU-DB:S3_ZVU-DB_S-ZVU" /></td>      <td>Stade</td>                             <td>                         22:08                           <span class="realtime-green">(+0)</span>                        </td>           </tr>
  #ROW:                    <tr class="odd">                        <td align="center"><img src="http://www.geofox.de/icon_service/line?height=14&lineKey=ZVU-DB:S21_ZVU-DB_S-ZVU" /></td>      <td>Elbgaustra�e</td>                       <td>                               22:10                           <span class="realtime-red">(+1)<span>                   </td>           </tr>
    if (m#<td align="center"><img src="([^"]+)" /></td>\s*<td>([^<]+)</td>\s*<td>\s*([\d:]+)\s*(<span class="realtime-(red|green)">\(([^<]+)\)</?span>)?\s*</td>\s*</tr>#) {
      #print "\nMATCH!\n\n\n";
      my $line_pic = $1;
      my $direction = $2;
      my $departure = $3;

      my $delay_color = $5;
      my $delay = $6;

      if ($line_pic =~ m#lineKey=[^:]+:([\d\w]+)_[\d\w_-]#) {
        my $line = $1;
        #my $direction = $western_to_utf8->convert($direction);

        printf("%s: %s %s ", $line, $western_to_utf8->convert($direction), $departure);
        print "(delay: $delay)"if defined($delay);
        print "\n";;

        my %connection = (
          station   => $station_ref->{name},
          direction => $direction,
          departure => $departure,
          line      => $line,
          line_pic  => $line_pic,
          ts        => time,
          index   => $result_counter,
        );
        $connection{delay} = $delay if ($delay);
        $connection{delay_color} = $delay_color if ($delay_color);

        my $json_string = encode_json(\%connection);
        system("mosquitto_pub -i hvv_scraper-$$ -t 'WEB/public_transport/schedule' -m '$json_string' $mqtt_tls -h $mqtt_broker -p $mqtt_port -u $mqtt_user -P '$mqtt_pass'");
      }
      last if (($station_ref->{display_lines}) && (++$result_counter >= $station_ref->{display_lines}));
    }
  }
}


sub startMqttSub {
    open MQTT_SUB, "mosquitto_sub -t 'WEB/public_transport/get' $mqtt_tls -h $mqtt_broker -p $mqtt_port -u $mqtt_user -P '$mqtt_pass'|" or die "could not open mqtt client";
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

sub mqttLoop {
    while (<MQTT_SUB>) {
      print "\n##################################################################\n\nNew MQTT Message: $_\n\n";
      getSchedule($_);
    }
}


# start the script..
print "[Ali's HVV <--> MQTT Gateway]\n";
print "startup...\n\n";

&startMqttSub;

my @threads;
my $t = threads->new(\&mqttLoop, 1);
push(@threads, $t);

foreach(@threads) {
    $_->join;
}

