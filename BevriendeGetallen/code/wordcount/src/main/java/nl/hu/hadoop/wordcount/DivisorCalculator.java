package main.java.nl.hu.hadoop.wordcount;

import java.util.ArrayList;

public class DivisorCalculator {

    public DivisorCalculator() {
    }

    public ArrayList<Integer> calculateDivisors(int number) {
        ArrayList<Integer> divisors = new ArrayList<Integer>();
        // we manually add one because the formula can't calculate that
        divisors.add(1);
        for (int i = 2; i <= number / 2; i++) {
            // ......  and we don't want the divider to be the same as the input number
            if (number % i == 0 && i != number) {
                divisors.add(i);
            }
        }

        return divisors;
    }
}