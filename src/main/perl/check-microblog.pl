#!/usr/bin/perl -w

use strict;

# Check a TREC 2015 microblog track retrieval track submission for various
# common errors:
#      * extra fields
#      * multiple run tags
#      * unknown topics (missing topics allowed, since retrieving no docs
#        for a topic is a valid repsonse)
#      * invalid retrieved documents (approximate check)
#      * duplicate retrieved documents in a single topic
#      * too many documents retrieved for a topic
# Messages regarding submission are printed to an error log

# Results input file differs for the two tasks
#     task a: topic-id tweet-id deliveryTime tag
#     task b: YYYYMMDD topic-id Q0 tweet-id rank score tag

# For task a, enforce that there are not more than 10 days * 10 tweets per
# day per topic. Can't enforce that those tweets actually fall
# on different days because need Tweet timestamp for that.  Also ensure
# that delvivery time is within the evaluation period (again,
# stricter rules apply but are too complicated to enforce here).

# For task b, enforce at most 100 Tweets per day per topic.  This check
# uses the YYYMMDD given in the run.  Note that this does not check that
# the Tweet actually occurred on that date.  That is beyond the scope of the
# check script.

# Change these variable values to the directory in which the error log
# should be put
my $errlog_dir = ".";

# If more than 25 errors, then stop processing; something drastically
# wrong with the file.
my $MAX_ERRORS = 25; 

my $MINTIME =  1437350400;	# Start and end of evaluation period
my $MAXTIME =  1438214399;	# Delivery times must be in this window

my $MAX_RET = 100;		# maximum number of docs to retrieve
			        # (per day for task b, total for task a)

my @topics;

my %docnos;                     # hash of retrieved docnos
my (%numret,%daycounts);	# number of docs retrieved per topic (per day)
my ($task,$results_file);	# task and input file to be checked
				# these are input arguments to the script
my $line;			# current input line
my $line_num;			# current input line number
my $errlog;			# file name of error log
my $num_errors;			# flags for errors detected
my $topic;
my ($docno,$q0,$sim,$rank,$tag,$date,$time);
my $q0warn = 0;
my $run_id;
my ($i);

my $usage = "Usage: $0 task resultsfile\n";
$#ARGV == 1 || die $usage;
$task = $ARGV[0];
$results_file = $ARGV[1];

$task eq "A" || $task eq "a" || $task eq "B" || $task eq "b" ||
	die "Invalid task: $task\n";
if ($task eq "A") {$task = "a";}
if ($task eq "B") {$task = "b";}

open RESULTS, "<$results_file" ||
	die "Unable to open results file $results_file: $!\n";

my @path = split "/", $results_file;
my $base = pop @path;
$errlog = $errlog_dir . "/" . $base . ".errlog";
open ERRLOG, ">$errlog" ||
	die "Cannot open error log for writing\n";

# Initialize data structures used in checks
@topics = map { sprintf "MB%03d", $_ } 226 .. 450;
foreach $topic (@topics) {
    $numret{$topic} = 0;
    for ($i=20; $i<=29; $i++) {
 	$date = "201507$i";
        $daycounts{$topic}{$date} = 0;
    }
}
$num_errors = 0;
$line_num = 0;
$run_id = "";
while ($line = <RESULTS>) {
    chomp $line;
    next if ($line =~ /^\s*$/);

    undef $tag;
    my @fields = split " ", $line;
    $line_num++;
	
    if ($task eq "a" ) {
	if (scalar(@fields) == 4) {
            ($topic,$docno,$time,$tag) = @fields;
        } else {
            &error("Wrong number of fields (expecting 4)");
             exit 255;
        }
    }
    else {
	if (scalar(@fields) == 7) {
            ($date,$topic,$q0,$docno,$rank,$sim,$tag) = @fields;
        } else {
            &error("Wrong number of fields (expecting 7)");
             exit 255;
        }
    }

    # make sure runtag is ok
    if (! $run_id) { 	# first line --- remember tag 
	$run_id = $tag;
	if ($run_id !~ /^[A-Za-z0-9_.-]{1,15}$/) {
	    &error("Run tag `$run_id' is malformed");
	    next;
	}
    }
    else {			# otherwise just make sure one tag used
	if ($tag ne $run_id) {
	    &error("Run tag inconsistent (`$tag' and `$run_id')");
	    next;
	}
    }

    # make sure topic is known
    if (!exists($numret{$topic})) {
	&error("Unknown topic '$topic'");
	$topic = 0;
	next;
    }  
    

    # make sure DOCNO known and not duplicated
    if ($docno =~ /^([0-9]){18}$/) { # valid DOCNO to the extent we will check
	if (exists $docnos{$docno} && $docnos{$docno} eq $topic) {
	    &error("Document `$docno' retrieved more than once for topic $topic");
	    next;
	}
	$docnos{$docno} = $topic;
    }
    else {				# invalid DOCNO
	&error("Unknown document `$docno'");
	next;
    }

    # for task a, make sure time is within the window and record retrieved doc
    if ($task eq "a") {
        if ($time < $MINTIME || $time > $MAXTIME) {
	    &error("Delivery time $time is outside evaluation window [$MINTIME,$MAXTIME]");
	    next;
	}

        $numret{$topic}++;
    }
    else {
    # task b checks and record retrieved doc by date
        if ($q0 ne "Q0" && ! $q0warn) {
            $q0warn = 1;
            &error("Field 3 is `$q0' not `Q0'");
	}

        # remove leading 0's from rank (but keep final 0!)
        $rank =~ s/^0*//;
        if (! $rank) {
            $rank = "0";
        }

        # date has to be in correct format and refer to a date within
        # the evaluation period.
	my ($year,$month,$day);
	if ($date !~ /(\d\d\d\d)(\d\d)(\d\d)/) {
	    &error("Date string $date not in correct format of YYYYMMDD");
	    next;
        }
	$year = $1; $month = $2; $day = $3;
	if ($year ne "2015") {
	    &error("Date $date has year $year, not 2015");
	    next;
	}
	if ($month ne "07" ) {
	    &error("Date $date has month $month, not 07");
	    next;
	}
	if ($day < 20 || $day > 29) {
	    &error("Date $date has day $day (must be between 20--29)");
	    next;
	}

	$daycounts{$topic}{$date}++;
    }
}


# Do global checks:
#   error if some topic has too many documents retrieved
if ($task eq "a") {
    foreach $topic (@topics) { 
        if ($numret{$topic} > $MAX_RET) {
print ERRLOG "$0 of $results_file:  WARNING: too many documents ($numret{$topic}) retrieved for topic $topic\n";
        }
    }
}
else {
    foreach $topic (@topics) { 
	for ($i=20; $i<=29; $i++) {
	    $date = "201507$i";
	    if ($daycounts{$topic}{$date} > $MAX_RET) {
                &error("Too many documents ($daycounts{$topic}{$date}) retrieved for day $date for topic $topic");
	    }
	}
    }
}

print ERRLOG "Finished processing $results_file\n";
close ERRLOG || die "Close failed for error log $errlog: $!\n";

if ($num_errors) { exit 255; }
exit 0;


# print error message, keeping track of total number of errors
sub error {
   my $msg_string = pop(@_);

    print ERRLOG 
    "run $results_file: Error on line $line_num --- $msg_string\n";

    $num_errors++;
    if ($num_errors > $MAX_ERRORS) {
        print ERRLOG "$0 of $results_file: Quit. Too many errors!\n";
        close ERRLOG ||
		die "Close failed for error log $errlog: $!\n";
	exit 255;
    }
}
