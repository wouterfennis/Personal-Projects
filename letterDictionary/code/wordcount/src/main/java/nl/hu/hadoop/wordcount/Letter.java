package main.java.nl.hu.hadoop.wordcount;

/**
 * Created by Wouter Fennis on 12-03-16.
 */
public class Letter {

    private char letterCharacter;
    private int totalLetterOccurrence;
    private int[] followingLetterOccurrence;

    public Letter(char letterCharacter, int totalLetterOccurrence, int[] followingLetterOccurrence) {
        this.letterCharacter = letterCharacter;
        this.totalLetterOccurrence = totalLetterOccurrence;
        this.followingLetterOccurrence = followingLetterOccurrence;
    }

    public double calculateFollowingLetterChance(int followingLetterIndex) {
        double chance = 0;
        double followingLetterOccurence = followingLetterOccurrence[followingLetterIndex];

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

    public int[] getFollowingLetterOccurrence() {
        return followingLetterOccurrence;
    }

    public void setFollowingLetterOccurrence(int[] followingLetterOccurrence) {
        this.followingLetterOccurrence = followingLetterOccurrence;
    }
}
