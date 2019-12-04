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

    my @files;
    for (my $i = 0; $i < @args; $i++) {
        push @files, CsvSelect::File->resultset($args[$i], $i+1);
    }
    my $file = shift @files;
    print "There are ",scalar(@files)," files\n";

    my $result = $file->outer_join(\@joins, @files);

    if ($show) {
        my @column_adjustment = CsvSelect::ResultSet->resolve_column_indexes_for_final_resultset($file, @files);

        my @show = split(',', $show);

        my @columns = map {
            my($fileno, $column) = split(':', $_);
            @column_adjustment[$fileno - 1] + $column - 1;
        } @show;

        $result->foreach(sub {
            my $row = shift;
            print join(', ', @$row[@columns]),"\n";
        });

    } else {
        # just show everything
        $result->foreach(sub {
            my $row = shift;
            print join(', ', @$row),"\n";
        });
    }
}

1;

