package CsvSelect;

use strict;
use warnings;

use Getopt::Long qw(GetOptionsFromArray);

use CsvSelect::File;
use CsvSelect::ResultSet;

sub run {
    my($self, @args) = @_;

    my($where, @joins, $show, $output);
    my @remaining = GetOptionsFromArray(\@args,
    #    'where=s' => \$where,
        'join=s'  => \@joins,
        'show=s'  => \$show,
        'output=s' => \$output,
    );

    my @files;
    for (my $i = 0; $i < @args; $i++) {
        push @files, CsvSelect::File->resultset($args[$i], $i+1);
    }
    my $file = shift @files;

    my $result = $file->outer_join(\@joins, @files);

    $result = $result->select($show);

    $output = '/dev/tty' unless ($output);
    $result->write_to($output);
}

1;

