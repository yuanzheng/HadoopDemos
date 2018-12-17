import org.apache.hadoop.mapreduce.lib.db.DBWritable;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/** Object for Database
 *
 */
public class DBOutputWritable implements DBWritable {

    private String inputPhrase;
    private String followingWord;
    private int count;

    public DBOutputWritable(String startingPhrase, String followingWord, int count) {

        this.inputPhrase = startingPhrase;
        this.followingWord = followingWord;
        this.count = count;
    }

    public void readFields(ResultSet arg0) throws SQLException {

        this.inputPhrase = arg0.getString(1);
        this.followingWord = arg0.getString(2);
        this.count = arg0.getInt(3);
    }

    public void write(PreparedStatement arg0) throws SQLException {

        arg0.setString(1, this.inputPhrase);
        arg0.setString(2, this.followingWord);
        arg0.setInt(3, this.count);
    }

}
