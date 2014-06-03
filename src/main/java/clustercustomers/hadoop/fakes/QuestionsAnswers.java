package clustercustomers.hadoop.fakes;

import com.google.common.collect.ImmutableMap;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: cscarion
 * Date: 03/06/2014
 * Time: 12:34
 * To change this template use File | Settings | File Templates.
 */
public class QuestionsAnswers {

    public static List<Map<String,String>> QUESTIONS_ANSWERS = new ArrayList<Map<String, String>>();
    public static Map<String,String> QUESTIONS_ANSWERS_MAP = ImmutableMap.of("Should I cover my employees?","Growth and progress often starts with new employees, but added responsibility can carry a few risks. If you employ one or more people, employers’ liability insurance is a legal requirement for your business. ","What is PL?","Public liability insurance is essential for most businesses, as it will keep you covered if a mistake is made which causes injury to a customer or member of the public, or damage to their property.","Do you cover for fire to my premises","We do and is important because Business revenues are disrupted as the business cannot remain open. In the United States in 2006 there were 1.6 million fires reported resulting in $11.3 billion in direct property loss. It is a risk that must be insured against.");
    static {
        QUESTIONS_ANSWERS.add(ImmutableMap.of("Should I cover my employees?","Growth and progress often starts with new employees, but added responsibility can carry a few risks. If you employ one or more people, employers’ liability insurance is a legal requirement for your business. "));
        QUESTIONS_ANSWERS.add(ImmutableMap.of("What is PL?","Public liability insurance is essential for most businesses, as it will keep you covered if a mistake is made which causes injury to a customer or member of the public, or damage to their property."));
        QUESTIONS_ANSWERS.add(ImmutableMap.of("Do you cover for fire to my premises","We do. And it is important because Business revenues are disrupted as the business cannot remain open. In the United States in 2006 there were 1.6 million fires reported resulting in $11.3 billion in direct property loss. It is a risk that must be insured against."));
    }
}
