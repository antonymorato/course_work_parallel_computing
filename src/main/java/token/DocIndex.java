package token;


import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;



@Setter
@Getter
@AllArgsConstructor
public class DocIndex {
    private String docID;
    private String[] terms;

}