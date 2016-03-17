package main.java.nl.hu.hadoop.wordcount;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Scanner;

/**
 * Created by Wouter Fennis on 12-03-16.
 */
public class DictionaryWordPredictor {

    final static char[] ALPHABET = {'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z'};
    final static String CSV_DELIMITER = ",";
    private ArrayList<Letter> letters = new ArrayList<Letter>();

    public DictionaryWordPredictor(String filePath) {
        readLetterOccurrencesFromFile(filePath);
    }

    public ArrayList<Letter> readLetterOccurrencesFromFile(String filePath) {
        BufferedReader bufferedReader;
        try {
            bufferedReader = new BufferedReader(new FileReader(filePath));

            try {
                String line = bufferedReader.readLine();
                while (line != null) {
                    Scanner lineScanner = new Scanner(line).useDelimiter(CSV_DELIMITER);
                    char letterCharacter = lineScanner.next().charAt(0);
                    int[] followingLetterOccurrences = new int[26];
                    int totalLetterOccurrence = 0;
                    int indexCounter = 0;

                    while (lineScanner.hasNext()) {

                        int Occurrence = Integer.parseInt(lineScanner.next());
                        if (lineScanner.hasNext()) {
                            followingLetterOccurrences[indexCounter] = Occurrence;
                        } else {
                            totalLetterOccurrence = Occurrence;
                        }
                        indexCounter++;
                    }
                    Letter newLetter = new Letter(letterCharacter, totalLetterOccurrence, followingLetterOccurrences);
                    letters.add(newLetter);
                    line = bufferedReader.readLine();
                }
            } catch (java.io.IOException error) {
                System.out.println("Het inlezen van een regel van het bestand is mislukt!");
            }
        } catch (FileNotFoundException error) {
            System.out.println("Inlezen van het bestand is mislukt!");
        }
        return letters;
    }

    // average predict method
/*    public double predict(String word) {
        // wordChance starts at 1.0 because if it would be 0.0 then you couldn't multiply it with anything
        double sumOfLetterChance = 0;
*//*        System.out.println("word");
        System.out.println(word);*//*
        for (int i = 0; i < word.length(); i++) {
            if (i + 1 < word.length()) {
                char firstCharacter = word.charAt(i);
                char nextCharacter = word.charAt(i + 1);
*//*                System.out.println("firstCharacter");
                System.out.println(firstCharacter);
                System.out.println("nextCharacter");
                System.out.println(nextCharacter);*//*
                int characterIndex = searchForCharacterIndex(nextCharacter);
*//*                System.out.println("characterIndex");
                System.out.println(characterIndex);*//*
                Letter letter = searchForLetterObject(firstCharacter);
                double letterChance = letter.calculateFollowingLetterChance(characterIndex);
                sumOfLetterChance = sumOfLetterChance + letterChance;
            } else {
                // the last character of the word has been reached
            }
        }
        // convert chance to percentage
        double averageWordPercentage = (sumOfLetterChance / word.length()) * 100;
        return averageWordPercentage;
    }*/

    //multiplying predict method
    public double predict(String word) {
        // wordChance starts at 1.0 because if it would be 0.0 then you couldn't multiply it with anything
        double wordChance = 1.0;
        System.out.println("word");
        System.out.println(word);
        for (int i = 0; i < word.length(); i++) {
            if (i + 1 < word.length()) {
                char firstCharacter = word.charAt(i);
                char nextCharacter = word.charAt(i + 1);
                System.out.println("firstCharacter");
                System.out.println(firstCharacter);
                System.out.println("nextCharacter");
                System.out.println(nextCharacter);
                int characterIndex = searchForCharacterIndex(nextCharacter);
                System.out.println("characterIndex");
                System.out.println(nextCharacter);
                Letter letter = searchForLetterObject(firstCharacter);
                double letterChance = letter.calculateFollowingLetterChance(characterIndex);
                wordChance = wordChance * letterChance;
            } else {
                // the last character of the word has been reached
            }
        }
        // convert chance to percentage
        double wordPercentage = wordChance * 100;
        return wordPercentage;
    }

    public Letter searchForLetterObject(char letterCharacter) {
        Letter wantedLetter = null;
        for (Letter possibleLetter : letters) {
            if (possibleLetter.getLetterCharacter() == letterCharacter) {
                wantedLetter = possibleLetter;
            }
        }
        return wantedLetter;
    }

    public int searchForCharacterIndex(char character){
        // -1 stands for "not found"
        int characterIndex = -1;

        for(int i = 0; i< ALPHABET.length; i++){
            if(ALPHABET[i] == character){
                characterIndex = i;
            }
        }
        return characterIndex;
    }


}
