package token;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;

import java.util.List;


@Getter
@Setter
@RequiredArgsConstructor()
public class Tokenizer {

    private String fileName;
    private String fileContents;

    private List<DocIndex> documentList;





}
