package CsvSelect::ResultSet;

use strict;
use warnings;

use Carp qw(croak);

use CsvSelect::Where;

sub new {
    my($class, $rows) = @_;

    return bless($rows, $class);
}

sub foreach {
    my($self, $code) = @_;

    for (my $i = 0; $i < @$self; $i++) {
        $code->($self->[$i]);
    }
}

sub width {
    my $self = shift;
    return scalar($self->[0]);
}

sub inner_join {
    my($self, $join_clauses, @resultsets) = @_;

    my @joins = $self->_parse_join_clauses($join_clauses);
    $self->_inner_join(1, \@joins, @resultsets);
}

sub _inner_join {
    my($self, $join_count, $joins, @resultsets) = @_;

    my $next_resultset = shift @resultsets;
    return $self unless $next_resultset;

    my @rows;
    $self->foreach(sub {
        my $left_row = shift;
        my $keep;
        $next_resultset->foreach( sub {
            my $right_row = shift;

            foreach my $join ( @{ $joins->[$join_count] } ) {
                return unless $join->($left_row, $right_row);
            }
            $keep = 1;
            push @rows, [ @$left_row, @$right_row ] if $keep;
        });
    });

    my $new_resultset = __PACKAGE__->new(\@rows);
    return $new_resultset->_inner_join($join_count+1, $joins, @resultsets);
}


sub _parse_join_clauses {
    my($self, $join_exprs, @resultsets) = @_;

    my @column_adjustment = $self->_resolve_column_indexes_for_final_resultset(@resultsets);

    my $operators = qr(=|<|>|<=|>=);

    my @joins;
    foreach my $join_clause ( @$join_exprs ) {
        my($left_fileno, $left_column, $operator, $right_fileno, $right_column) = $join_clause =~ m/(\d+):(\d+)($operators)(\d+):(\d+)/;
        unless (defined $left_fileno) {
            croak "Can't parse join: $join_clause";
        }

        $left_column += $column_adjustment[$left_fileno - 1];

        my $join = CsvSelect::Where->new("${left_fileno}:${left_column}", $operator, "${right_fileno}:${right_column}");
        my $join_idx = _max_fileno($left_fileno, $right_fileno) - 1; # -1 because it's 0-based
        $joins[$join_idx] ||= [];
        push @{ $joins[$join_idx] }, $join;
    }

    return @joins;
}


sub _max_fileno {
    my($left, $right) = @_;

    return( $left > $right ? $left : $right );
}

# As we join more resultsets on the right, we need to shift the column indexes
# up to account for the columns already joined on the left
sub _resolve_column_indexes_for_final_resultset {
    my($self, @resultsets) = @_;

    my @column_adjustment = ( 0 );
    for (my $i = 1; $i < @resultsets; $i++) {
        push @column_adjustment, $resultsets[$i-1]->width();
    }
    return @column_adjustment;
}

1;

