public class Driver {


    public static void main(String[] args) throws Exception {

        DataDividerByUser dataDividerByUser = new DataDividerByUser();
        /*
        CoOccurrenceMatrixGenerator coOccurrenceMatrixGenerator = new CoOccurrenceMatrixGenerator();
        Normalization normalization = new Normalization();

         */
        /* TODO Multiplication and Sum */


        String rawInput = args[0];
        String userMovieListOutputDir = args[1];
        //String coOccurrenceMatrixOutputDir = args[2];
        //String normalizationDir = args[3];

        String[] path1 = {rawInput, userMovieListOutputDir};
        //String[] path2 = {userMovieListOutputDir, coOccurrenceMatrixOutputDir};
        //String[] path3 = {coOccurrenceMatrixOutputDir, normalizationDir};

        DataDividerByUser.main(path1);

    }

}
