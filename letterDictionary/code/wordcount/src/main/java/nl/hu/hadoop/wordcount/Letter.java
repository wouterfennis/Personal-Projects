package main.java.nl.hu.hadoop.wordcount;

/**
 * Created by Wouter Fennis on 12-03-16.
 */
public class Letter {

    private char letterCharacter;
    private int totalLetterOccurrence;
    private int[] followingLetterOccurrences;

    public Letter(char letterCharacter, int totalLetterOccurrence, int[] followingLetterOccurrences) {
        this.letterCharacter = letterCharacter;
        this.totalLetterOccurrence = totalLetterOccurrence;
        this.followingLetterOccurrences = followingLetterOccurrences;
    }

    public double calculateFollowingLetterChance(int followingLetterIndex) {
        double chance = 0;
        double followingLetterOccurence = followingLetterOccurrences[followingLetterIndex];

        if (followingLetterOccurence != 0) {
            chance = followingLetterOccurence / ((double)totalLetterOccurrence);
        }
        return chance;
    }

    public char getLetterCharacter() {
        return letterCharacter;
    }

    public void setLetterCharacter(char letterCharacter) {
        this.letterCharacter = letterCharacter;
    }

    public int getTotalLetterOccurrence() {
        return totalLetterOccurrence;
    }

    public void setTotalLetterOccurrence(int totalLetterOccurrence) {
        this.totalLetterOccurrence = totalLetterOccurrence;
    }

    public int[] getFollowingLetterOccurrences() {
        return followingLetterOccurrences;
    }

    public void setFollowingLetterOccurrences(int[] followingLetterOccurrences) {
        this.followingLetterOccurrences = followingLetterOccurrences;
    }
}
