package songs_analyzer;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.List;

import static java.util.Arrays.asList;
import static org.junit.Assert.*;

/**
 * @author Evgeny Borisov
 */
@RunWith(SpringRunner.class)
@ContextConfiguration(classes = Conf.class)
@ActiveProfiles("DEV")
public class TopXServiceTest {

    @Autowired
    private UserConf userConf;

    @Autowired
    JavaSparkContext sc;

    @Autowired
    TopXService topXService;


    @Test
    public void topX() {
        JavaRDD<String> rdd = sc.parallelize(asList("java java scala", "scala java"));
        List<String> list = topXService.topX(rdd, 1);
        Assert.assertEquals(1,list.size());
        Assert.assertEquals("java",list.get(0));
    }
}












