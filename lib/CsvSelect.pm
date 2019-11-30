package CsvSelect;

use strict;
use warnings;

use Getopt::Long qw(GetOptionsFromArray);

use CsvSelect::File;
use CsvSelect::ResultSet;

sub run {
    my @args = @_;

    my($where, $join, $show);
    GetOptionsFromArray(\@args,
        'where=s' => \$where,
        'join=s'  => \$join,
        'show=s'  => \$show,
    );

    my @files = map { CsvSelect::File->resultset($_) } @ARGV;

    $files[0]->foreach(sub {
        my $row = shift;
        print join(', ', @$row),"\n";
    });
}

1;

