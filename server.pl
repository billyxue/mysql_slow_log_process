#!/usr/bin/perl

# server : recv slow query from client
# Author : billy @ taomee
# Email: cracker0@126.com


use warnings;
use strict;
use DBI;
use POSIX qw(:signal_h WNOHANG setsid strftime); 
use Fcntl qw(:DEFAULT :flock); 
use Term::ANSIColor qw(:constants color);
use IO::Socket ;


my $rundir   = '.'; 
my $pid_file = $rundir . "/recv.pid"; 
my $log_dir  = $rundir . "/log";
my $err_log  = $log_dir. "/recv.err"; 
my $log_file = $log_dir. "/recv.log"; 

mkdir($log_dir, 0755) unless -d $log_dir;

my $host = "192.168.xxx.xx";
my $port = 6677 ;
my $save_dir = './current' ;

my $database='db_slow';
my $dbport = '3456';
my $user= 'backup';
my $password = 'xxxxxx';

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

my ($dsn, $dbh, $sth ); 
$dsn = "DBI:mysql:database=$database;host=$host;port=$dbport";
$dbh = DBI->connect($dsn, $user, $password,
                    { PrintError => 0, RaiseError => 1 }); 

my $get_proj_sql = "select proj_name from t_proj a join t_slow_config b on (a.proj_id = b.proj_id) where host_ip='%s';";

if (! -d $save_dir) 
{
	mkdir($save_dir,0755) ;
	write_log "Save directory created: $save_dir" ;
}

my $server = IO::Socket::INET->new(
	Listen => 50,
	LocalAddr => $host,
	LocalPort => $port ,
	Proto     => 'tcp'
) or die "Can't create server socket: $!";

write_log "Server opened: $host:$port\nWaiting clients...\n" ;


	# temporary solution
	my $proj_dir;
	my $keepdir;
	my $remote_host_ip;

	write_log "New client!" ;
	my $buffer;
	my $data_content = "" ;
	my $buffer_size = 1 ;
	my %data = (
		filename => "", 
		filesize => 0 , 
		filesave => "",
	);

	my $proj_name;
	while( sysread($client, $buffer , $buffer_size) ) 
	{
		if    ($data{filename} !~ /#:#$/) 
		{ 
			$data{filename} .= $buffer ; 
		}
		elsif ($data{filesize} !~ /_$/) 
		{ 
			$data{filesize} .= $buffer ;
		}
		elsif ( length($data_content) < $data{filesize}) 
		{
			$remote_host_ip = (split /\_/, $data{"filename"})[0] ;
			#print "From : $remote_host_ip\n";

			my $sql = sprintf($get_proj_sql, $remote_host_ip);

			my $d_r = $dbh->selectrow_hashref($sql);

			if ( $d_r->{"proj_name"} eq "" ) {
				 log_warn "[WARN] No such proj or not register\n";
				 $proj_name = "other";
			}else{
				$proj_name = $d_r->{"proj_name"};
			}

			$proj_dir = $save_dir .'/'.$proj_name;
			mkdir($proj_dir,0755) unless -d $proj_dir;

			$keepdir = $proj_dir .'/'. strftime("%Y%m%d", localtime );
			mkdir($keepdir,0755) unless -d $keepdir ;

			if ($data{filesave} eq '') 
			{
				$data{filesave} = "$keepdir/$data{filename}" ;
				$data{filesave} =~ s/#:#$// ;
				$buffer_size = 1024*10 ;
				if (-e $data{filesave}) { unlink ($data{filesave}) ;}
				write_log "Saving: $data{filesave} ($data{filesize}bytes)" ;
			}

			open (FILENEW,">>$data{filesave}") ; 
			binmode(FILENEW) ;
			print FILENEW $buffer ;
			close (FILENEW) ;
			print "." ;        
		}
		else {  last ;}
	}
	close($client);
	write_log "--OK--" ;
}



