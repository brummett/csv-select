package CsvSelect;

use strict;
use warnings;

use Getopt::Long qw(GetOptionsFromArray);

use CsvSelect::File;
use CsvSelect::ResultSet;

sub run {
    my($self, @args) = @_;

    my($where, @joins, $show);
    my @remaining = GetOptionsFromArray(\@args,
    #    'where=s' => \$where,
        'join=s'  => \@joins,
        'show=s'  => \$show,
    );

    my($file, @files) = map { CsvSelect::File->resultset($_) } @args;

    my $result = $file->inner_join(\@joins, @files);

    $result->foreach(sub {
        my $row = shift;
        print join(', ', @$row),"\n";
    });
}

1;

