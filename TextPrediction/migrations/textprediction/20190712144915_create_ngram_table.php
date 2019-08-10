<?php
date_default_timezone_set('America/Denver');
use Phinx\Migration\AbstractMigration;

class CreateNgramTable extends AbstractMigration
{

    /**
     * Migrate Up.
     */
    public function up()
    {
$sql = <<<ENDSQL
CREATE TABLE `ngram`  (
    `starting_phrase` varchar(250) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
    `following_word` varchar(250) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
    `count` int(11) NOT NULL
) ENGINE = InnoDB CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic;

ENDSQL;

        $this->execute($sql);
    }

    /**
     * Migrate Down.
     */
    public function down()
    {
$sql = <<<ENDSQL

DROP TABLE IF EXISTS `ngram`;

ENDSQL;

    $this->execute($sql);

    }
}
