package songs_analyzer;

import lombok.Getter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import scala.Serializable;
import scala.util.parsing.combinator.testing.Str;

import java.util.List;

import static java.util.Arrays.asList;

/**
 * @author Evgeny Borisov
 */
@Component
public class UserConf implements Serializable {

    @Getter
    private List<String> garbage;

    @Value("${garbage}")
    public void setGarbage(String[] garbage) {
        this.garbage = asList(garbage);
    }
}
