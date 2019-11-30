package CsvSelect::Where;

use strict;
use warnings;
use Carp qw(croak);
use Scalar::Util qw(looks_like_number);

my %op_to_constructor = (
    '=' => 'eq',
);

sub new {
    my($class, $left, $op, $right) = @_;

    my $constructor = $op_to_constructor{$op};
    unless ($constructor) {
        croak "Unknown operator $op";
    }

    if (_is_constant($left)) {
        $constructor .= '_const';
        ($right, $left) = ($left, $right);
    } elsif (_is_constant($right)) {
        $constructor .= '_const';
    }

    return $class->$constructor($left, $right);
}

sub _is_constant {
    my $val = shift;
    return $val !~ m/(\d+):(\d+)/;
}

# Returns a sub that expects 2 args, each is a row from
# one of the files
sub eq {
    my($class, $left, $right) = @_;

    my(undef, $left_col) = split(':', $left);
    my(undef, $right_col) = split(':', $right);
    $left_col--;
    $right_col--;
    return sub {
        my $left_val = $_[0]->[$left_col];
        my $right_val = $_[1]->[$right_col];

        if (looks_like_number($left_val) and looks_like_number($right_val)) {
            return( $left_val == $right_val );
        } else {
            return( $left_val eq $right_val );
        }
    }
}

sub eq_const {
    my($class, $column_ref, $const_value) = @_;

    # column_ref is a string like 1:5, meaning look at the 5th column of the 1st file
    my(undef, $column) = split(':', $column_ref);

    if (looks_like_number($const_value)) {
        return sub {
            my $val = $_[0]->[$column];
            if (looks_like_number($val)) {
                return( $val == $const_value );
            } else {
                return( $val eq $const_value );
            }
        }
    } else {
        return sub {
            return( $_[0]->[$column] eq $const_value );
        }
    }
}

