public class Driver {


    public static void main(String[] args) throws Exception {

        DataDividerByUser dataDividerByUser = new DataDividerByUser();
        CoOccurrenceMatrixGenerator coOccurrenceMatrixGenerator = new CoOccurrenceMatrixGenerator();
        //Normalization normalization = new Normalization();

        String rawInput = args[0];
        String userMovieListOutputDir = args[1];
        String coOccurrenceMatrixOutputDir = args[2];
        String normalizationDir = args[3];
        String multiplicationDir = args[4];
        String sumDir = args[5];

        String[] path1 = {rawInput, userMovieListOutputDir};
        String[] path2 = {userMovieListOutputDir, coOccurrenceMatrixOutputDir};
        String[] path3 = {coOccurrenceMatrixOutputDir, normalizationDir};
        String[] path4 = {normalizationDir, rawInput, multiplicationDir};
        String[] path5 = {multiplicationDir, sumDir};

        DataDividerByUser.main(path1);
        CoOccurrenceMatrixGenerator.main(path2);
        Normalization.main(path3);
        MatrixMultiplication.main(path4);

        /* TODO Multiplication and Sum */

    }

}
