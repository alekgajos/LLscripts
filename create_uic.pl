use strict;
use warnings;
use English;

############################################################
# prepare text to be written to the uic file
#############################################################
sub uic_header_generator {
  my ($ntu_file_path) = @_;
  my $uic_header = <<END_UIC_HEADER;
!
set verify
use emcdbini runictl interf/par=1
talk interf cutchar no cutneu no onlytruemc no makecuts yes exit
talk runictl offmag yes 5.18797 ACCICORR no GAMEFFICORR no return
filt interf on
hist open/max_nrec=65536/recl=4095/file="$ntu_file_path"
hist/mod=interf on
END_UIC_HEADER
  return $uic_header;
}

sub uic_footer_generator {
  my $uic_footer = <<END_UIC_FOOTER;
hist write
hist close
hist del
sh all
exit
!
END_UIC_FOOTER
  return $uic_footer;
}

##########################################################################
# Prepare the LoadLeveler CMD file                                       #
##########################################################################
sub cmd_header_generator {
  my ($root_path, $datamc) = @_;
  my $cmd_header = <<END_CMD_HEADER;
#!/bin/ksh 
#@ job_name = kaon_t_cpt
#@ job_type = serial
#@ INTROOT = $root_path
#@ input = /dev/null 
#@ output = \$(INTROOT)/prod/log/${datamc}_\$(JobId).\$(StepId).out
#@ error = \$(INTROOT)/prod/log/${datamc}_\$(JobId).\$(StepId).err
#@ notification = error
#@ requirements = (Arch == "R6000") && (OpSys == "AIX53" || OpSys == "AIX71") && (Machine != {"fibm45.lnf.infn.it"}) && (Machine != {"fibm19.lnf.infn.it"}) && (Machine != {"fibm20.lnf.infn.it"})
END_CMD_HEADER
return $cmd_header;
}

sub recall_job_generator {
  my ($datamc, $recall_id, $recall_dependency, $first_run, $last_run) = @_;

  my $dependency_line = $recall_dependency > 0 ? "#@ dependency = (recall_batch_$recall_dependency == 0)" : "";

  my $job = <<END_JOB;
############### RECALLING BATCH $recall_id ######################
#@ class = gmtu
#@ step_name = recall_batch_$recall_id
$dependency_line
#@ arguments = $first_run $last_run
#@ executable = \$(INTROOT)/scripts/$datamc\_load.ksh
#@ queue
END_JOB
  return $job;
}

sub analysis_job_generator {
  my ($datamc, $uic_file_number, $recall_dependency) = @_;

  my $job = <<END_JOB;
############### ANALYSING FILE $uic_file_number ######################
#@ class = kuser
#@ step_name = analysis_$uic_file_number
#@ dependency = (recall_batch_$recall_dependency == 0)  
#@ arguments = $uic_file_number
#@ executable = \$(INTROOT)/scripts/run_$datamc.csh
#@ queue
END_JOB
  return $job;
}

##########################################################################
# Handling of options                                                    #
##########################################################################

my $num_args = $#ARGV + 1;
if ($num_args != 2 and $num_args != 3) {
  print "\nUsage: create_uic.pl first_run last_run [use_recalled_only] \n";
  exit;
}

my $first_run=$ARGV[0];
my $last_run=$ARGV[1];
my $datarec_version=26;
my $mc_datarec_version=26;
my $recalled_datarecs_only = 0;
if ( $num_args == 3) {
  $recalled_datarecs_only=$ARGV[2];
}

#############################################################
# useful variables
#############################################################
my $stream = 42;
my $mc_stream = 62;
my $base_data_file = "data_stream".$stream."_";
my $base_mc_file = "mc_stream".$mc_stream."_mccard2_";
my $lumi_factor = 431.;
my $min_lumi = 20.;
#my $lumi_limit = 300.0;	# nb-1
my $lumi_limit = 1500.0;	# nb-1
# run quality cuts
my $sqrts_limit = 3.;
my $k00pm_limit = 3.;

# get base path and detailed paths
my $path = `pwd`;
$path =~ s/\n/\/..\//g;
my $uic_path = $path."prod\/uic\/";
#my $ntu_path = $path."ntu\/";
my $ntu_path = "\/gpfs\/group\/gajos\/refchannels\/kpienucr\/ntu\/";
my $data_cmd_file_path = $path."/scripts/data_batch.cmd";
my $mc_cmd_file_path = $path."/scripts/mc_batch.cmd";

# query database about the runs with datarecs
my $data_query = "dbonl \"select distinct RUN_NR, REAL(sum(distinct VLABHA))/$lumi_factor as LUM from LOGGER.DATAREC_RUNS where RUN_NR between $first_run and $last_run and VERSION=$datarec_version";

# check for existence of corresponding MC files
$data_query .= " and RUN_NR in (SELECT RUN_NR from LOGGER.DATAREC_LOGGER where VERSION=$mc_datarec_version and STREAM_ID=$mc_stream and GB_NR=0 and FILENAME like \'\%%all_phys_\%\%' and RUN_NR between $first_run and $last_run)";

# check run quality
$data_query .= " and RUN_NR in (select RUN_NR from CNDRUN.QUALITY where abs(k00pm_s) <= $k00pm_limit and abs(hepdbsqrts_s) <= $sqrts_limit and RUN_NR between $first_run and $last_run)";

if ( $recalled_datarecs_only ) {
  $data_query .= " and RUN_NR NOT IN (SELECT CASE WHEN missing <> 0 THEN RUN_NR END FROM (SELECT RUN_NR,COUNT(filename) AS missing FROM (SELECT RUN_NR,FILENAME from LOGGER.DATAREC_DATA WHERE RUN_NR between $first_run and $last_run and stream_id=$stream and version=$datarec_version EXCEPT (SELECT RUN_NR,FILENAME from EXPSTAT.DATAREC_RECALLED WHERE RUN_NR between $first_run and $last_run and stream_id=$stream and version=$datarec_version)) GROUP BY RUN_NR) )";
}

$data_query .= "group by RUN_NR having (REAL(sum(distinct VLABHA))/$lumi_factor) >= $min_lumi";

$data_query .= "\"";

my $query_result = `$data_query`;

# extract run numbers from the query result
my %run_list;

my @query_result = split /\n/, $query_result;
foreach my $line (@query_result) { 
  if ( $line =~ m/(\d+)\s+(\d+\.*\d*)/ ) {
    $run_list{$1} = $2;
  }
}

# query database about the MC runs

#############################################################
# generate the actual uic files
# and generate a single LL command file in parallel
#############################################################
#

# stuff for UIC generation
my $i = 0;
my $run_counter = 0;
my $total_lumi = 0.;
my $ntuple_lumi = $lumi_limit + 1.0;

my $data_ntu_file_path = "";
my $data_uic_file_path = "";

my $mc_ntu_file_path = "";
my $mc_uic_file_path = "";

my $data_uic_content = "";
my $mc_uic_content = "";

# bookkeeping for generation of the CMD files
my $first_run_in_batch = -1;
my $last_run_in_batch = -1;
my $recall_job = 0;
my %recall_batches;
my $prev_run;
my $prev_i = 1;

for my $run_no (sort { $a cmp $b } (keys %run_list)) {

  if($first_run_in_batch == -1){
    $first_run_in_batch = $run_no;
  }

  $run_counter = $run_counter + 1;

  my $run_lumi = $run_list{$run_no};

  if ( $ntuple_lumi + $run_lumi < $lumi_limit ) { # append run to current ntuple


  } else {
    # close and write previous file
    if ( $i > 0 ) {
      $data_uic_content .= uic_footer_generator();
      $mc_uic_content .= uic_footer_generator();
    
      # write to file
      open(my $file, '>', $data_uic_file_path) or die "Could not open UIC file: $!";
      print $file $data_uic_content;
      close $file;
    
      print "Written UIC file: $data_uic_file_path\n";

      open(my $file2, '>', $mc_uic_file_path) or die "Could not open UIC file: $!";
      print $file2 $mc_uic_content;
      close $file2;

      print "Written UIC file: $mc_uic_file_path\n";

      # save information about a new recalling batch for 40 analysis jobs
      if($i%40 == 0){
	$recall_job++;
	$last_run_in_batch = $prev_run;
	$recall_batches{$recall_job} = [$first_run_in_batch, $last_run_in_batch, $prev_i, $i];
	$first_run_in_batch = $run_no;
	$last_run_in_batch = $prev_run;
	$prev_i = $i+1;
      }
    }

    #reset counters
    $ntuple_lumi = 0.;
    
    #start a new ntuple
    $i = $i + 1;
    $data_ntu_file_path = $ntu_path . $base_data_file . $i . ".ntu";
    $data_uic_file_path = $uic_path . $base_data_file . $i . ".uic";

    $mc_ntu_file_path = $ntu_path . $base_mc_file . $i . ".ntu";
    $mc_uic_file_path = $uic_path . $base_mc_file . $i . ".uic";
    
    $data_uic_content = uic_header_generator($data_ntu_file_path);

    $mc_uic_content = uic_header_generator($mc_ntu_file_path);

  }

  # add the current run to present ntuples
  $data_uic_content .= "input url \"dbdatarec: (STREAM_ID=$stream) and (RUN_NR=$run_no)"
    ." and (VERSION=$datarec_version) and (ARCHIVED=2)\"\n";

  $mc_uic_content .= "input url \"dbmcdst: (DTR_STREAM_ID=$mc_stream) and (RUN_NR=$run_no)"
    ." and (DTR_VERSION=$mc_datarec_version) and (DTR_ARCHIVED=2) and (MC_MCCARD_ID=2) and (DTR_GB_NR=0)\"\n";

  $data_uic_content .= "beg\n";
  $mc_uic_content .= "beg\n";

  $ntuple_lumi = $ntuple_lumi + $run_lumi;
  $total_lumi = $total_lumi + $run_lumi;

  print $run_counter, "\t", $run_no, "\t", $run_lumi, "\t", $ntuple_lumi, "\t", $total_lumi, "\n";

  $prev_run = $run_no;
}

# handle the last, still open, UIC files

$data_uic_content .= uic_footer_generator();
$mc_uic_content .= uic_footer_generator();

# write to file
open(my $file, '>', $data_uic_file_path) or die "Could not open UIC file: $!";
print $file $data_uic_content;
close $file;

print "Written UIC file: $data_uic_file_path\n";

open(my $file2, '>', $mc_uic_file_path) or die "Could not open UIC file: $!";
print $file2 $mc_uic_content;
close $file2;

print "Written UIC file: $mc_uic_file_path\n";


# handle the last, possibly-incomplete batch of recalling

$last_run_in_batch = $prev_run;
$recall_job++;
$recall_batches{$recall_job} = [$first_run_in_batch, $last_run_in_batch, $prev_i, $i];

##########################################################################
# Write the LoadLeveler CMD files                                        #
##########################################################################
# stuff for CMD generation
open(my $mc_cmd_file, '>', $mc_cmd_file_path) or die "Could not open CMD file: $!";
open(my $data_cmd_file, '>', $data_cmd_file_path) or die "Could not open CMD file: $!";

print $mc_cmd_file cmd_header_generator($path, 'mc');
print $data_cmd_file cmd_header_generator($path, 'data');

my $previous_recall_batch = -1;

for my $batch_number (sort { $a <=> $b } (keys %recall_batches)) {

  my ($first_run, $last_run, $first_ntu, $last_ntu) = @{$recall_batches{$batch_number}};

#  print $mc_cmd_file recall_job_generator('mc', $batch_number, $batch_number - 1, $first_run, $last_run);
#  print $data_cmd_file recall_job_generator('data', $batch_number, $batch_number - 1, $first_run, $last_run);
 
  print $mc_cmd_file recall_job_generator('mc', $batch_number, $previous_recall_batch, $first_run, $last_run);
  print $data_cmd_file recall_job_generator('data', $batch_number, $previous_recall_batch, $first_run, $last_run);
 
  $previous_recall_batch = $batch_number;

  for(my $i = $first_ntu;$i<=$last_ntu;$i++){
    # generate analysis job for the UIC files
    print $mc_cmd_file analysis_job_generator('mc', $i, $batch_number);
    print $data_cmd_file analysis_job_generator('data', $i, $batch_number);
  }
  
}

close $data_cmd_file;
close $mc_cmd_file;






















