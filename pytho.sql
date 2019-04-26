CREATE TABLE IF NOT EXISTS `corpora`.`weibo_oracle` ( `kp` INT NOT NULL AUTO_INCREMENT , `hash` BIGINT NOT NULL , `content` TEXT NOT NULL , `pub_date` DATETIME NOT NULL , `ret_date` DATETIME NOT NULL , `processed` TINYINT NOT NULL , `marked_by_proc` INT NOT NULL , `bag_of_words` TEXT, `likes` SMALLINT, `shares` SMALLINT, `comments` SMALLINT, `nickname` TINYTEXT, `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP , PRIMARY KEY (`kp`), INDEX (`hash`)) ENGINE = InnoDB CHARSET=utf8 COLLATE utf8_general_ci;

CREATE TABLE IF NOT EXISTS `corpora`.`weibo_oracle_xref` ( `oracle_kp` INT NOT NULL , `topic` TEXT NOT NULL , `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP , INDEX (`topic`)) ENGINE = InnoDB;

