#
# Copyright (c) 2003-2006 University of Chicago and Fellowship
# for Interpretations of Genomes. All Rights Reserved.
#
# This file is part of the SEED Toolkit.
# 
# The SEED Toolkit is free software. You can redistribute
# it and/or modify it under the terms of the SEED Toolkit
# Public License. 
#
# You should have received a copy of the SEED Toolkit Public License
# along with this program; if not write to the University of Chicago
# at info@ci.uchicago.edu or the Fellowship for Interpretation of
# Genomes at veronika@thefig.info or download a copy from
# http://www.theseed.org/LICENSE.TXT.
#


package Sim;
#use FIGAttributes;
#use base 'FIGAttributes';

use overload '""' => \&as_string;

sub as_string
{
    my($obj) = @_;

    return sprintf("sim:%s->%s:%s", $obj->id1, $obj->id2, $obj->psc);
}

sub new_from_line
{
    my($class, $line) = @_;

    chomp $line;
    my $self = [split(/\t/, $line)];
    return bless $self, $class;
}

sub validate
{
    my($self) = @_;

    return ($self->id1 ne "" and
	    $self->id2 ne "" and
	    $self->iden =~ /^[.\d]+$/ and
	    $self->ali_ln =~ /^\d+$/ and
	    $self->mismatches =~ /^\d+$/ and
	    $self->gaps =~ /^\d+$/ and
	    $self->b1 =~ /^\d+$/ and
	    $self->e1 =~ /^\d+$/ and
	    $self->b2 =~ /^\d+$/ and
	    $self->e2 =~ /^\d+$/ and
	    $self->psc =~ /^[-.e\d]+$/ and
	    $self->bsc =~ /^[-.\d]+$/ and
	    $self->ln1 =~ /^\d+$/ and
	    $self->ln2 =~ /^\d+$/);
}


sub as_line
{
    my($self) = @_;

    return join("\t", @$self) . "\n";
}



sub id1  {
    my($sim) = @_;
    return $sim->[0];
}

sub id2  {
    my($sim) = @_;
    return $sim->[1];
}

sub iden  {
    my($sim) = @_;
    return $sim->[2];
}

sub ali_ln  {
    my($sim) = @_;
    return $sim->[3];
}

sub mismatches  {
    my($sim) = @_;
    return $sim->[4];
}

sub gaps  {
    my($sim) = @_;
    return $sim->[5];
}

sub b1  {
    my($sim) = @_;
    return $sim->[6];
}

sub e1  {
    my($sim) = @_;
    return $sim->[7];
}

sub b2  {
    my($sim) = @_;
    return $sim->[8];
}

sub e2  {
    my($sim) = @_;
    return $sim->[9];
}

sub psc  {
    my($sim) = @_;
    return ($sim->[10] =~ /^e-/) ? "1.0" . $sim->[10] : $sim->[10];
}

sub bsc  {
    my($sim) = @_;
    return $sim->[11];
}

sub bit_score  {
    my($sim) = @_;
    return $sim->[11];
}

sub ln1  {
    my($sim) = @_;
    return $sim->[12];
}

sub ln2  {
    my($sim) = @_;
    return $sim->[13];
}

sub tool  {
    my($sim) = @_;
    return $sim->[14];
}

sub def2  {
    my($sim) = @_;
    return $sim->[15];
}

sub ali  {
    my($sim) = @_;
    return $sim->[16];
}

1
