package com.datastax.example;


import java.io.*;
import java.nio.charset.Charset;
import java.nio.file.*;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/*
Copyright 2014 Patrick McFadin

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

Program to process ISD-lite data files from NOAA and put them into an easier to ingest form.

Files from the ftp site will come as one file per weather station, per year. This processor will:
    - Convert position dependant to csv
    - Apply the proper scaling to each field. Decimals are not in pos dependant files, so they must be divided by a factor of 10.
    - Combine all station files for a year into a single file
    - GZip the final file

 */

public class NOAAFileProcessor {


    public static void main(String[] args) {

        if (args.length < 1) {

            System.out.println("Usage: NOAAFileProcessor <data_directory> <output_directory>");
            System.exit(-1);
        }

        String dataDirectory = args[0];
        String outputDirectory = null;

        if (args.length == 2)
            outputDirectory = args[1];


        List<String> dirs = directoryList(dataDirectory);

        for (String dir : dirs) {
            if (outputDirectory != null) {
                processDirectory(fileList(dir), outputDirectory);
            } else {
                processDirectory(fileList(dir), dir);
            }
        }

    }

    static void processDirectory(List<String> files, String path) {

        Path inputPath;

        PrintStream p;

        String year = path.substring(path.lastIndexOf('/'));
        String outfile = path + year + ".csv.gz";

        int fileCount = 0;
        System.out.println("From directory: " + path);
        System.out.print("To file: " + outfile + " ");

        for (String file : files) {
            if (file.equalsIgnoreCase(outfile)) {
                continue;
            }

            inputPath = FileSystems.getDefault().getPath(file);
            fileCount++;

            try {
                //Open output file
                p = new PrintStream(new GZIPOutputStream(new FileOutputStream(outfile, true)), true);


                // Open a input stream to be uncompressed
                InputStream in = Files.newInputStream(inputPath);
                GZIPInputStream gis = new GZIPInputStream(in);

                // Use a buffered reader
                Reader decoder = new InputStreamReader(gis, Charset.defaultCharset());
                BufferedReader reader = new BufferedReader(decoder);

                // Read each line from the input file, transform and put into single file
                String line;
                while ((line = reader.readLine()) != null) {
                    p.println((getWeatherStationCode(file) + "," + parseLine(line)[1]));
                }

                in.close();
                p.close();
            } catch (Exception e) {
                System.out.println("Error while reading file: " + file);
                e.printStackTrace();

            }
            if (fileCount % 100 == 0) {
                System.out.print(".");
            }

        }
        System.out.println(" - Total files: " + fileCount);
    }

    /*
       Routine to get data from distinct positions, format and do some cleaning.
       Array is returned with a unique key and string formatted as a CSV for output.
     */
    static String[] parseLine(String line) {

        String[] retVal = new String[2];

        // Extract data by position in file.
        String year = line.substring(0, 4).trim();
        String month = line.substring(5, 7).trim();
        String day = line.substring(8, 10).trim();
        String hour = line.substring(11, 13).trim();
        float temp = getRealNumber(line.substring(14, 20).trim(), 10);
        float dewPoint = getRealNumber(line.substring(21, 25).trim(), 10);
        float pressure = getRealNumber(line.substring(26, 31).trim(), 10);
        int windDir = (int) getRealNumber(line.substring(32, 37).trim(), 1);
        float windSpeed = getRealNumber(line.substring(38, 43).trim(), 10);
        int conditionCode = (int) getRealNumber(line.substring(44, 49).trim(), 1);
        float oneHourPrecip = getRealNumber(line.substring(50, 55).trim(), 10);
        float sixHourPrecip = getRealNumber(line.substring(56, 61).trim(), 10);


        retVal[0] = (year + month + day + hour);

        retVal[1] = (year + "," + month + "," + day + "," + hour + "," + temp + "," + dewPoint + "," + pressure + "," + windDir + "," + windSpeed + "," + conditionCode + "," + oneHourPrecip + "," + sixHourPrecip);

        return retVal;
    }

    /*
       Takes two weather station codes in file and combines for a single unique identifier.
     */
    static String getWeatherStationCode(String path) throws Exception {
        String key = "";


        // Take something like this: /Users/patrick/projects/weather_data/2004/032040-99999-2004.gz
        // and get this: 032040:99999

        String fileName = new File(path).getName();

        String[] filenameArray = fileName.split("-");


        return filenameArray[0] + ":" + filenameArray[1];

    }

    /*
       Each file has -9999 values that indicate no readings taken.
       No floats in the original file so a scaling has to be applied
    */
    static float getRealNumber(String value, int scaling) {

        float val = Float.parseFloat(value);

        if (value.equalsIgnoreCase("-9999")) {
            val = 0;
        } else if (val != 0) {
            val = Float.parseFloat(value) / scaling;
        }

        return val;
    }

    /*
       Returns a list of directories in a given path
     */
    static List<String> directoryList(String directory) {

        List<String> dirs = new ArrayList<>();
        try {
            DirectoryStream<Path> ds = Files.newDirectoryStream(Paths.get(directory));
            for (Path path : ds) {
                if (Files.isDirectory(path)) {
                    dirs.add(path.toString());
                }
            }
        } catch (IOException ex) {
            ex.printStackTrace();
        }

        return dirs;
    }

    /*
       Returns a list of files for a given path
     */
    static List<String> fileList(String directory) {

        List<String> files = new ArrayList<>();
        try {
            DirectoryStream<Path> ds = Files.newDirectoryStream(Paths.get(directory), "*.gz");
            for (Path path : ds) {
                files.add(path.toString());
            }
        } catch (IOException ex) {
            ex.printStackTrace();
        }
        return files;
    }
}
