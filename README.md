# vlp

A Vietnamese text processing library developed in the Scala programming language.

## 0. Introduction

This is a repository of a Scala project which implements some basic tasks of Vietnamese text processing.
Each basic task is implemented in a module. 
- `tok`: tokenizer, which implements a rule-based word segmentation approach;
- `tag`: tagger, which implements a conditional Markov model for sequence tagging;
- `ner`: named entity recognizer, which implements a bidirectional conditional Markov model for sequence tagging;
- `tdp`: dependency parser, which implements a transition-based dependency parsing approach;

## 1. Tokenizer

The tokenizer module is bundled in the file `tok.jar`. See section `Compile and Package` below to know how to create this jar file from source.

The main class of the tokenizer module is `vlp.tok.Tokenizer`. It segments a given text into tokens. Each token is represented by a triple (`position`, `shape`, `content`). This class can take two arguments of an input text file and an output file. The input file must exist and contain plain text, arranged in lines. The output file will be created by the program. For example:

  `$java -jar tok.jar path/to/inp.txt path/to/out.txt`

If the output file is not provided, the result will be shown to the console. If both the input and output files are not provided, a sample sentence will be processed and its result is shown to the console.  

The tokenizer makes use of parallel processing in Scala, which effectively exploits **all CPU cores** of a single machine. For this reason, on large file it is still fast. On my laptop, the tokenizer can process an input file of more than 532,000 sentences (about 1,000,000 syllables) in about 100 seconds.

For really large input files in a big data setting, it is more convenient to use the tokenizer together with the [Apache Spark](http://spark.apache.org) library so that it is easily to port to a cluster of multiple nodes. We provide a transformer-based implementation of the Vietnamese tokenizer, in the class `vlp.tok.TokenizerTransformer`. This can be integrated into the machine learning pipeline of the Apache Spark Machine Learning library, in the same way as the standard `org.apache.spark.ml.feature.Tokenizer`. Note that the wrapper transformer depends on Apache Spark but not the tokenizer. If you do not want to use Apache Spark, you can simply copy the self-contained tokenizer and import it to your project, delete `TokenizerTransformer` and ignore all Apache Spark dependencies.

## 2. Part-of-Speech Tagger

The tagger module implements a simple first-order conditional Markov model (CMM) for sequence tagging. The basic features include current word, previous word, next word, current word shape, next word shape, previous previous word, and next next word. Each local transition probability is specified by a multinomial logistic regression model.

On the standard VLSP 2010 part-of-speech tagged treebank, this simple model gives a training accuracy is 0.9638 when all the corpus is used for training. A pre-trained model is provided in the directory `dat/tag`.

Since the machine learning pipeline in use is that of Apache Spark, this module depends on Apache Spark. Suppose that you have alreadly a version of Apache Spark installed (say, version 2.4.5). 

### 2.1. Tagging Mode

To tag an input text file containing sentences, each on a line, invoke the command

  `$spark-submit tag.jar -m tag -i dat/sample.txt`

Option `-m` specifies the running mode. If the pre-trained model is not on the default path, you must specify it explicitly with option `-p`, as follows:

  `$spark-submit tag.jar -m tag -i dat/sample.txt -p path/to/model`

Note that the input text file does not need to be tokenized in advanced, the tagger will call the tokenizer module to segment the text into words before tagging.

### 2.2. Training Mode

To train a model, you will need the VLSP 2010 part-of-speech tagged corpus (which has about 10,000 manually tagged sentences). Suppose that the corpus is provided at the default path `dat/vtb-tagged.txt`:

  `$spark-submit tag.jar -m train`

If the data is at another location, specify it with option `-d`:

  `$spark-submit tag.jar -m train -d path/to/tagged/corpus`

The resulting model will be saved to its default directory `dat/tag`. This can be changed with option `-p` as above. After training, the evaluation mode is called automatically to print out performance scores (accuracy and f-score) on the training set.

There are some other options for fine-tuning the training, such as `-f` (for min feature frequency cutoff, default value is 3) or `-u` (for domain dimension, default value is 16,384). See the code for detail.

By default, the master URL is set to `local[*]`, which means that all CPU cores of the current machine are used by Apache Spark. You can specify a custom master URL with option `-M`.

## 3. Named Entity Recognizer

The named entity recognition module implements a bidirectional conditional Markov model for sequence tagging. This tagging model combines a forward CMM and a backward CMM which are trained independently and then combined in decoding. This method has achieved the best F1 score of the [VLSP 2016 shared task on Vietnamese Named Entity Recognition](https://vlsp.org.vn/vlsp2016/eval/ner). On the standard test set of VLSP 2016 NER, its F1 score is about 88.8%.

The detailed approach is described in the following paper:

* [Vietnamese Named Entity Recognition using Token Regular Expressions and Bidirectional Inference](https://arxiv.org/abs/1610.05652), Phuong Le-Hong, Proceedings of Vietnamese Speech and Language Processing (VLSP), Hanoi, Vietnam, 2016.

As the `tag` module, the `ner` module is also an Apache Spark application, you run it by submitting the main JAR file `ner.jar` to Apache Spark. The main class of the toolkit is `vlp.ner.Tagger` which selects the desired tool by following arguments provided by the user.  

The arguments are as follows:

  * `-M <master>`: the master URL for Apache Spark, default is `local[*]` which uses all CPU cores of the current machine. If run on a cluster, you should provide the Spark master URL here, for example `-M spark://192.168.1.1:7077`.
  * `-l <language>`: the language to process, where `language` is an abbreviation of language name which is either `vie` (Vietnamese) or `eng` (English). If this argument is not specified, the default language is Vietnamese.  
  * `-v`: this parameter does not require argument. If it is used, the module runs in verbose mode, in which some intermediate information will be printed out during the processing. This is useful for debugging.
  * `-m <mode>`: the running mode, either `tag`, `train`, `eval`, or `test`; the default mode is `tag`.
  * `-i <input-file>`: the name of an input file to be used. If running in the `eval` or `train` mode, this should be a file in the CoNLL format for NER. If running in `tag` mode, it should be a raw text file in UTF-8 encoding, each sentence is on a line.
  * `-u <dimension>`: this argument is only required in the `train` mode to specify the number of features (or the domain dimension) of the resulting CMM. The dimension is a positive integer and depends on the size of the data. Normally, the larger the training data is, the greater the dimension that should be used. As an example, we set this argument as 32,768 when training a CMM on about 16,000 tagged sentences of the VLSP NER corpus. The default dimension is 32,768.
  * `-r`: this parameter does not require argument. If it is used, the tagger will train or test using reversed sentences to produce a backward sequence model instead of the default forward sequence model.

### 3.1. Tagging Mode ###

To tag an input file and write the result to an output file of the same name (with generated suffix `.out`), using the default pre-trained model:

  `$spark-submit ner.jar -m tag -i <input-file>` 

To evaluate the accuracy on a gold corpus `vie.test`:

  `$spark-submit ner.jar -m eval -i path/to/vie.test`

This will produces an output file `vie.test.out` in the same directory as `vie.test`. This is a two-column text file in the format ready for being evaluated with the `conlleval` script. Running with `conlleval vie.test.out` should gives a result similar to:

```
processed 66097 tokens with 2996 phrases; found: 3038 phrases; correct: 2675.
accuracy:  99.02%; precision:  88.05%; recall:  89.29%; FB1:  88.66
              LOC: precision:  87.48%; recall:  93.76%; FB1:  90.51  1478
             MISC: precision:  80.95%; recall:  69.39%; FB1:  74.73  42
              ORG: precision:  71.93%; recall:  44.89%; FB1:  55.28  171
              PER: precision:  90.94%; recall:  94.67%; FB1:  92.77  1347
```

### 3.2. Training Mode

To train a forward model tagger on a gold corpus at the path `dat/ner/vie/vie.train`:

  `$spark-submit ner.jar -m train -u 4096`

The resulting model will be saved in the default directory `dat/ner/vie/cmm-f`.

To train a backward model tagger on a gold corpus at the path `dat/ner/vie/vie.train`:

  `$spark-submit ner.jar -m train -u 4096 -r`

The resulting model will be saved in the default directory `dat/ner/vie/cmm-b`.

The default forward and backward CMM models are provided in the directory `dat/ner/vie`, which use 32,768 dimensions (that is, they are trained with the option `-u 32768`).

On a large dataset, in order to avoid the out-of-memory error, you should consider to use the option `--driver-memory` of Apache Spark when submitting the job, as follows: 

  `$spark-submit --driver-memory 16g ner.jar -m train -l eng`

## 4. Dependency Parser

The dependency parser module implements a transition-based dependency parsing algorithm. The `vlp.tdp.Classifier` learns a mapping from a parsing context to a labeled transition. Training samples in the form of (parsing context, labeled transition) pairs are extracted automatically from an available treebank in the CoNLLU format, as defined by the [Universal Dependency](http://universaldependencies.org) project. This classifier implements both a multinomial logistic regression (MLR) model and a multi-layer perceptron (MLP) model for classification. 

Two dependency treebanks, one for English and one for Vietnamese are available in the `dat/dep/eng` and `dat/dep/vie`. These datasets of version 2.0 are publicly available at the [Universal Dependency](http://universaldependencies.org) website. In the Vietnamese treebank, there are 1,400 training sentences and 800 development sentences. On this small dataset, the transition classifier with MLR achieves a training accuracy of about 87.57% and a development accuracy of about 78.61% when fixing the number of features at 1,024. The MLP model gives a better scores of 79.08% (development) and 89.82% (training). Using a bigger number of features gives a higher training accuracy but a lower development accuracy. Refer to the class `vlp.tdp.FeatureExtractor` for the list of (discrete) features used in this classifier implementation.

### 4.1. Transition Classifier

To train a transition classifier using MLR model with default settings:

  `$spark-submit --class vlp.tdp.Classifier tdp.jar -m train`

The resulting model will be saved to its default directory `dat/tdp/vie/mlr`. After training, the evaluation mode is called automatically to print out development and training scores.

To train the classifier using MLP with default settings:

  `$spark-submit --class vlp.tdp.Classifier tdp.jar -m train -c mlp`

The option `-c` stands for classifier type. To use a MLP with two hidden layers, the first layer has 64 units and the second layer has 32 units, use the `-h` option as follows:

`$spark-submit --class vlp.tdp.Classifier tdp.jar -m train -c mlp -h "64 32"`

The resulting model will be saved to its default directory `dat/tdp/vie/mlp`.

There are some other options for fine-tuning the training, such as `-f` (for min feature frequency cutoff, default value is 3) or `-u` (for domain dimension, default value is 1,024). See the code for detail.

To train a transition classifier for English, use the option `-l eng` (`-l` is for language). For example:

`$spark-submit --class vlp.tdp.Classifier tdp.jar -m train -l eng -u 2048`

The resulting model will be saved to its default directory `dat/tdp/eng/mlr`.

As above, by default, the master URL is set to `local[*]`, which means that all CPU cores of the current machine are used by Apache Spark. You can specify a custom master URL with option `-M`. On a large dataset such as the English treebank, in order to avoid the out-of-memory error, you should consider to use the option `--driver-memory` of Apache Spark when submitting the job, as follows: 

`$spark-submit --driver-memory 16g --class vlp.tdp.Classifier tdp.jar -m train -l eng -u 16384`

The executor memory is set to default value of `8g`.

### 4.2. Parser


## Compile and Package

### Notes
- Most of the modules depends on the Machine Learning library of Apache Spark.
- This big data technology permits to process millions of texts with a very high speed.
- The services can be used in two modes: batch processing (offline) or on-the-fly (online).
- The program is developed in the Scala programming language. It needs a Java Runtime Environment (JRE) 
  to run, or a Java Development Kit (JDK) environment to compile and package. We use Java version 8.0.
- Since the code is developed in Scala, you need to have Scala too.
- If you want to compile and build the software from source, you need a Scala build tool 
  to manage all dependencies and produce a binary JAR file. We use [SBT](https://www.scala-sbt.org/download.html).

### Assembly
- Go to the main directory `cd vlp` on your command line console.
- Invoke `sbt` console with the command `sbt`.
- In the sbt, compile the entire project with the command `compile`. All requried libraries are automatically downloaded, only at the first time.
- In the sbt, package the project with the command `assembly`.
- The resulting JAR files are in sub-projects, for example 
  * tokenizer is in `tok/target/scala-2.11/tok.jar`
  * part-of-speech tagger is in `tag/target/scala-2.11/tag.jar`
  * named entity tagger is in `ner/target/scala-2.11/ner.jar`
  * etc.

## Contact

Any bug reports, suggestions and collaborations are welcome. I am
reachable at: 
* LE-HONG Phuong, http://mim.hus.edu.vn/lhp/
* College of Science, Vietnam National University, Hanoi