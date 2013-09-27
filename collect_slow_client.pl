#!/usr/bin/env perl 
#######################
# prefetch db slow log
# 
# CHANGELOG :
#  Created by  Billy @ taomee.com  2012-05-28
#  Email: cracker0@126.com
# 
#######################

use warnings;
use strict; 
use DBI;
use Data::Dumper;
use File::stat qw(stat);
use IO::Socket; 
use File::Copy qw(move);
use POSIX qw(:signal_h WNOHANG setsid strftime); 
use English qw(-no_match_vars);
use Fcntl qw(:DEFAULT :flock); 
use Net::SMTP; 
use Term::ANSIColor qw(:constants color);
use Switch;

my $debug=1;
my $program = $0;
use constant DO_THOLD => 128;

my $cfg_file = $ARGV[0];
my $rundir   = '.'; 
my $pid_file = $rundir . "dbslow.pid"; 
my $log_dir  = $rundir . "/log";
my $err_log  = $log_dir. "/dbslow.err"; 
my $log_file = $log_dir. "/dbslow.log"; 
my $slog_st  = $rundir . "/cur";
my $arlog_st = $rundir . "/archive";

# decide slow log file & name
my $slow_log_file;
my $slow_log_name;
my $slow_log_size;
my $slow_log_newfile;
my $slow_log_newname;
my $last_send_t = 0;

# TODO: control trans speed smartly
our $speed = 2*1024*1024; 

our %need_para=();
our @valid_para =qw/
mgm_db_ip
mgm_db
mgm_db_port 
mgm_db_user 
mgm_db_pass 
slow_table
/;

#### important variables ####
our ( $mgm_db_ip, $mgm_db, $mgm_db_user, $mgm_db_pass, $mgm_db_port , $slow_table );
my $mgm_dbh;
my $local_ip ;
my ( $srv_host, $srv_port );
my ( $local_addr, $local_user, $local_pass, $local_socket, $local_port );
# poor man 's code
$local_addr = "localhost";

my $local_dbh;
our $infor_db = "information_schema";
our $SOCKET;
my $datadir;

my $check_slow_on = "SHOW GLOBAL VARIABLES LIKE 'log_slow_queries'";
my $check_datadir = "SHOW GLOBAL VARIABLES LIKE 'datadir'";

#### temporary unsed var  #####
my %status;

mkdir($log_dir, 0755) unless -d $log_dir;
mkdir($slog_st, 0755) unless -d $slog_st;
mkdir($arlog_st,0755) unless -d $arlog_st;

if ( $#ARGV )
{
	print BOLD, GREEN, "Usage: $program <config file>\n", RESET;
	exit(2);
}

### functions ###
sub write_log
{ 
    my $time=scalar localtime; 
    open (HDW,">>",$log_file); 
    flock (HDW,LOCK_EX); 
    print HDW $time,"  ",join ' ',@_,"\n"; 
    flock (HDW,LOCK_UN); 
    close HDW; 
}

# write the error log when call die
sub log_die
{ 
    my $time=scalar localtime; 
    open (HDW,">>",$err_log); 
    print HDW BOLD, RED, $time,"[Crit]",@_ ,RESET; 
    close HDW; 
    die @_; 
}

sub log_warn 
{ 
    my $time=scalar localtime; 
    open (HDW,">>",$err_log); 
    print HDW BOLD, YELLOW, $time," [WARN] ",@_, RESET; 
    close HDW; 
}

$SIG{__DIE__}=\&log_die; 
$SIG{__WARN__}=\&log_warn; 

sub daemon 
{ 
    my $child = fork(); 
    log_die "[EMERG] can't fork\n" unless defined $child; 
    exit 0 if $child; 
    setsid(); 
     
    open (STDIN, "</dev/null"); 
    open (STDOUT, ">/dev/null"); 
    open (STDERR,">&STDOUT"); 
 
    chdir $rundir; 
    umask(022); 
    $ENV{PATH}='/export/dbbak:/db_bak:/bin:/usr/bin:/sbin:/usr/sbin'; 
 
    return $$; 
}

sub valid_para
{
	no strict "refs";
	my ($item, $array_ref) = @_;
	foreach my $it (@$array_ref ) 
	{
		if ( $item eq $it ) 
		{
			return 1;
		}
	}
	return 0;
}

sub load_config 
{
	log_die "Can't find config file: $cfg_file : $!\n" unless -e $cfg_file;
	my $config_file = shift;
	open (CONFIG, "<$config_file")
		or die "Can't open the $config_file $!";
	while(<CONFIG>)
	{
		chomp;
		next if $_ =~ /^#/ || $_ =~ /^$/ || $_ =~ /^\[/;
		my ($key, $value) = (split /=/,$_);
		#print("$key - $value\n");
		if( &valid_para($key, \@valid_para) ) 
		{
			$need_para{$key} = $value;
		} 
		else
		{
			log_warn "not valid argu:[$key]\n";
		}
		#TODO: process option para
	}
	$mgm_db_ip   = $need_para{mgm_db_ip};
	$mgm_db      = $need_para{mgm_db};
	$mgm_db_port = $need_para{mgm_db_port};
	$mgm_db_user = $need_para{mgm_db_user};
	$mgm_db_pass = $need_para{mgm_db_pass};
	$slow_table  = $need_para{slow_table};
}

sub get_local_ip
{
	$local_ip = qx#/sbin/ifconfig eth1|grep 'inet addr'|grep  -v '127.0.0.1'|tail -1|awk '{print \$2}'|cut -d":" -f2 #;
	if ($local_ip eq "") 
	{
		$local_ip = qx#/sbin/ifconfig bond0 |grep 'inet addr'|grep  -v '127.0.0.1'|tail -1|awk '{print \$2}'|cut -d":" -f2 #;
		if ( $local_ip eq "")
		{
			return 0;
		}
	} 
	else 
	{
		$local_ip =~s/\n| / /;
		$local_ip =~s/[a-zA-Z]//g;
		$local_ip =~s/\s$//g;
		write_log "Get Local IP:$local_ip";
		return 1;
	}
}

sub get_conn
{
        my ( $host, $port, $user, $pass, $db , $dbh) = @_;
        #print "$host, $port, $user, $pass, db=$db , $dbh\n";
        my $dsn= "DBI:mysql:database=$db;host=$host;port=$port";
#TODO: add timeout option
	eval 
	{
		$$dbh = DBI->connect($dsn, $user, $pass)
			or die "Can't connect DB $!";
	};
	return $EVAL_ERROR ? 1 : 0;
}

sub flush_slowlog
{
	write_log "flush logs";
	eval { $local_dbh->do("FLUSH LOGS") or die "failed when flush logs: $!"; };
	return $EVAL_ERROR ? 1 : 0;
}

sub get_socket
{
	write_log "Enter get socket";
	my ($addr, $port, $SOCKET) = @_;
	eval 
	{
		$$SOCKET = IO::Socket::INET->new(
			PeerAddr => "$addr",
			PeerPort => "$port",
			#Type  => SOCK_STREAM,
			Proto => "tcp",
			timeout => 30,
		) or die "$@";
	};
	return $EVAL_ERROR ? 1 : 0;
}

sub guess_slowlog_file_name
{
	my $slow_log_conf_name = qx#cat /etc/mysql/my.cnf|grep ^log_slow_queries#;
	if ( $slow_log_conf_name eq "" )
	{
		# not found any config in my.cnf  
		log_warn "May remove slow log config my.cnf\n";
		return 1;
	}
	elsif ($slow_log_conf_name !~ /=/)
	{
		$slow_log_file = sprintf("%s%s-slow.log",$datadir,$local_ip);
		$slow_log_name = sprintf("%s-slow.log", $local_ip);
		return 0;
	}
	else
	{
		$slow_log_conf_name =~ /(.*)=\s*(\S+)/;
		my $tempfile = $2;
		write_log "[my.cnf] Slow Log Name:$tempfile";
		if ( $tempfile !~ /\// )
		{
			$slow_log_file = $datadir.$tempfile;
			$slow_log_name = $tempfile;
		}
		else
		{
			$slow_log_file = $tempfile;
			$slow_log_name = (split /\//, $slow_log_file)[-1];
		}
		return 0;
	}
}

sub send_file 
{
	write_log "Enter send_file";
	my $i = 10;
        my ( $file , $filename , $host , $port ) = @_ ;
	write_log "start to send file: $file";
        if (! -s $file) { log_die "ERROR! Can't find or blank file: $file" ;}
	while ( $i )
	{
		my $server = &get_socket($host,$port, \$SOCKET);
		if ( $server ) 
		{
			log_warn "can't connect to store srv [$host:$port]\n";
			sleep(5);
			next;
		}
		last;
	}

	my $file_size = -s $file ;
	my $file_name = $filename;

	$SOCKET->autoflush(1);

	write_log "Sending [$file_name] size [$file_size] bytes" ;
	print $SOCKET "$file_name#:#";
	print $SOCKET "$file_size\_" ;
	write_log "start to trans file : $file";
	open (FILE,$file); 
	binmode(FILE);
	my $buffer;
	while( sysread(FILE, $buffer , $speed) ) 
	{
		print $SOCKET $buffer;
		sleep(1) ;
	}
	close (FILE) ;
	close ($SOCKET) ;
	write_log "--------Send Done---------\n" ;
	return 0;
}

########### daemonrize ############
if (-e $pid_file) 
{ 
    open (PIDFILE,$pid_file) or log_die "[EMERG] $!\n"; 
    my $pid=<PIDFILE>; 
    close PIDFILE; 
 
    log_die "[EMERG] process is still run\n" if kill 0 => $pid; 
    log_die "[EMERG] can't remove pid file\n" unless -w $pid_file && unlink $pid_file; 
} 

my $now =strftime("%Y-%m-%d %H:%M:%S", localtime );
open (HDW,">",$pid_file) or log_die "[EMERG] $!\n"; 
my $pid = daemon(); 
print HDW $pid; 
close HDW; 

######### START ########
START:
&load_config($cfg_file);

if ( !get_local_ip() )
{
	log_die "can't get local ip(eth1 or bond),please check!!\n";
}

my $get_mgm_db_conn = &get_conn($mgm_db_ip,$mgm_db_port,$mgm_db_user,$mgm_db_pass,$mgm_db,\$mgm_dbh);
log_die "can't connect to mgm host\n" if $get_mgm_db_conn;
write_log "Connect to mgm host OK";

my $get_reporter = "SELECT id, host_ip, host_port, host_socket , \
	db_user, db_pass, slow_collect_flag, slog_center_host ,slog_center_port \ 
	FROM $mgm_db.$slow_table WHERE slow_collect_flag = 1 and host_ip='$local_ip' LIMIT 1";
my @db_rec = @{ $mgm_dbh->selectall_arrayref($get_reporter, { Slice => {} }) };

# check if not reg in the srv
if (! @db_rec)
{
	log_warn "This Server May not need to process slow log\nsleep 300 and retry\n";
	#sleep(300);
	sleep(300);
	goto START;
}
for(@db_rec)
{
	$srv_host     = $_->{'slog_center_host'};
	$srv_port     = $_->{'slog_center_port'};
	$local_port   = $_->{'host_port'};
	$local_socket = $_->{'host_socket'};
	$local_user   = $_->{'db_user'};
	$local_pass   = $_->{'db_pass'};
}

write_log "server_host : $srv_host server_port : $srv_port";
write_log "local db info[upsP]: $local_user, $local_pass, $local_socket, $local_port";

my $local_db_conn = &get_conn($local_addr,$local_port,$local_user,$local_pass,$infor_db,\$local_dbh);
log_die "can't connect to local db\n" if $local_db_conn;
write_log "Connect to local MYSQL OK";

my $h_r = $local_dbh->selectrow_hashref($check_slow_on);
my $d_r = $local_dbh->selectrow_hashref($check_datadir);

$datadir = $d_r->{"Value"};
write_log "mysql datadir: $datadir";
while ( $h_r->{"Value"} ne "ON" )
{
	log_warn "Slow log disabled, Sleep 300 and Retry\n";
	sleep(300);
}

while ( &guess_slowlog_file_name() )
{
	log_warn "may not set slow query in my.cnf\n";
	sleep(30);
}
write_log "Slow log file is : $slow_log_file";
write_log "Slow log name is : $slow_log_name";


#while ( (time - $last_send_t) < 120 )
while ( (time - $last_send_t) < 3600 )
{
	write_log "just sent , sleeping ...";
	sleep(300);
	#sleep(30);
}

while ( !(strftime("%H", localtime ) eq "01") ) 
{
	write_log "time [01pm] is not reachable";
	sleep(300);
}

CHECKSIZE:
# check if real exists in datadir
if ( -e $slow_log_file && -r _ && -w _ )
{
	my $st = stat($slow_log_file);
	$slow_log_size = $st->size;
	#TODO: collect md5
	write_log "slow log size : $slow_log_size";
	# rename missing: Invalid cross-device link  move instead temporary
	if ( $slow_log_size < DO_THOLD )
	{
		write_log "slow file is small, skip and wait";
		sleep(5);
		goto CHECKSIZE;
	}

	write_log "slow file big enough , start to send";
	$slow_log_newfile = sprintf("%s/%s_%s" ,$slog_st , $local_ip, strftime("%Y%m%d", localtime ) );
	$slow_log_newname = sprintf("%s_%s" ,$local_ip, strftime("%Y%m%d", localtime ) );
	write_log "New slow_log File: $slow_log_newfile Name: $slow_log_newname";

	REMOVE:	
	if ( !move($slow_log_file, $slow_log_newfile) )
	{
		log_warn "HELP!! failed when move old slow logfile : $!\n";
		log_warn "I don't know how to resolv this , Contact DBA";
		# TODO: send msg to server and sms DBA
		while(1){ sleep(20); goto REMOVE; }
	}

	my $fret = &flush_slowlog();
	if ( $fret )
	{
		log_warn  "flush logs failed : $!\n" ;
		write_log "flush logs failed : $!";
	}
	else
	{
		write_log "flush logs ok";
	}

	# send file
	my $ssret = &send_file( $slow_log_newfile, $slow_log_newname, $srv_host, $srv_port ) ;
	$last_send_t = time;
	sleep(2);
	unless ( $ssret )
	{
		my $slow_log_archfile = sprintf("%s/%s_%s" ,$arlog_st, $local_ip, strftime("%Y%m%d", localtime ) );
		if ( !move($slow_log_newfile, $slow_log_archfile) )
		{
			log_warn "HELP!! failed when move to archive dir : $!\n";
		}
	}

	# for safety
	goto START;
}
else
{
	log_warn "There is no slowlog named $slow_log_file in There\n";
	log_warn "Try to flush logs";
	my $fret = &flush_slowlog();
	if ( $fret )
	{
		log_warn "flush logs failed : $!" 
	}
	else
	{
		write_log "flush logs ok";
	}
	goto START;
}


##########
exit(0);
##########
