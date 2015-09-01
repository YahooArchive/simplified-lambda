// Copyright 2015 Yahoo Inc.
// Licensed under the terms of the Apache 2.0 License
// See LICENSE.txt file in the project root folder for License terms.

package flickr.SimplifiedLambdaDemo;

import flickr.SimplifiedLambda.SimplifiedLambda;
import utility.MockHTable;
import org.apache.hadoop.hbase.client.HTableInterface;

/**
 * Created by bjoshi on 8/18/15.
 */
public class SimplifiedLambdaDemo {
    // mvn exec:java -Dexec.mainClass="flickr.SimplifiedLambdaDemo.SimplifiedLambdaDemo"
    public static void main(final String[] args) throws Exception {
        printHeader();

        MockHTable lambdaTable = new MockHTable("lambdaTable");
        lambdaTable.addColumnFamily(new String(SimplifiedLambda.FAMILY));

        SimplifiedLambda simplifiedLambda = new SimplifiedLambda(lambdaTable);

        System.out.println("\nInitial table:");
        System.out.print(simplifiedLambda.dumpTable());

        System.out.println("\nPush bulk rows:");
        simplifiedLambda.pushBulkEntry("rowA", "bulk");
        simplifiedLambda.pushBulkEntry("rowB", "bulk");
        simplifiedLambda.pushBulkEntry("rowC", "bulk");
        simplifiedLambda.cleaner();
        System.out.print(simplifiedLambda.dumpTable());

        System.out.println("\nOverride rows A,B with realtime:");
        simplifiedLambda.pushRealtimeEntry("rowA", "rtOvrd");
        simplifiedLambda.pushRealtimeEntry("rowB", "rtOvrd");
        System.out.print(simplifiedLambda.dumpTable());

        System.out.println("\nClean table:");
        simplifiedLambda.cleaner();
        System.out.print(simplifiedLambda.dumpTable());

        System.out.println("\nOverride rows B,C with realtime:");
        simplifiedLambda.pushRealtimeEntry("rowB", "rtOvrd2");
        simplifiedLambda.pushRealtimeEntry("rowC", "rtOvrd2");
        System.out.print(simplifiedLambda.dumpTable());

        System.out.println("\nOverride all rows with bulk:");
        simplifiedLambda.pushBulkEntry("rowA", "bulkOvr");
        simplifiedLambda.pushBulkEntry("rowB", "bulkOvr");
        simplifiedLambda.cleaner();
        System.out.print(simplifiedLambda.dumpTable());

    }


    static private void printHeader() {
        System.out.println("                     _");
        System.out.println("           . -  ` : `   '.' ``  .            - '` ` .");
        System.out.println("         ' ,gi$@$q  pggq   pggq .            ' pggq");
        System.out.println("        + j@@@P*\\7  @@@@   @@@@         _    : @@@@ !  ._  , .  _  - .");
        System.out.println("     . .  @@@K      @@@@        ;  -` `_,_ ` . @@@@ ;/           ` _,,_ `");
        System.out.println("     ; pgg@@@@gggq  @@@@   @@@@ .' ,iS@@@@@Si  @@@@  .6@@@P' !!!! j!!!!7 ;");
        System.out.println("       @@@@@@@@@@@  @@@@   @@@@ ` j@@@P*\"*+Y7  @@@@ .6@@@P   !!!!47*\"*+;");
        System.out.println("     `_   @@@@      @@@@   @@@@  .@@@7  .   `  @@@@.6@@@P  ` !!!!;  .    '");
        System.out.println("       .  @@@@   '  @@@@   @@@@  :@@@!  !:     @@@@7@@@K  `; !!!!  '  ` '");
        System.out.println("          @@@@   .  @@@@   @@@@  `%@@@.     .  @@@@`7@@@b  . !!!!  :");
        System.out.println("       !  @@@@      @@@@   @@@@   \\@@@$+,,+4b  @@@@ `7@@@b   !!!!");
        System.out.println("          @@@@   :  @@@@   @@@@    `7%S@@hX!P' @@@@  `7@@@b  !!!!  .");
        System.out.println("       :  \"\"\"\"      \"\"\"\"   \"\"\"\"  :.   `^\"^`    \"\"\"\"   `\"\"\"\"\" ''''");
        System.out.println("        ` -  .   .       _._    `                 _._        _  . -");
        System.out.println("                , ` ,glllllllllg,    `-: '    .~ . . . ~.  `");
        System.out.println("                 ,jlllllllllllllllp,  .!'  .+. . . . . . .+. `.");
        System.out.println("              ` jllllllllllllllllllll  `  +. . . . . . . . .+  .");
        System.out.println("            .  jllllllllllllllllllllll   . . . . . . . . . . .");
        System.out.println("              .l@@@@@@@lllllllllllllll. j. . . . . . . :::::::l `");
        System.out.println("            ; ;@@@@@@@@@@@@@@@@@@@lllll :. . :::::::::::::::::: ;");
        System.out.println("              :l@@@@@@@@@@@@@@@@@@@@@l; ::::::::::::::::::::::;");
        System.out.println("            `  Y@@@@@@@@@@@@@@@@@@@@@P   :::::::::::::::::::::  '");
        System.out.println("             -  Y@@@@@@@@@@@@@@@@@@@P  .  :::::::::::::::::::  .");
        System.out.println("                 `*@@@@@@@@@@@@@@@*` `  `  `:::::::::::::::`");
        System.out.println("                `.  `*%@@@@@@@%*`  .      `  `+:::::::::+`  '");
        System.out.println("                    .    ```   _ '          - .   ```     -");
        System.out.println("                       `  '                     `  '  `");
        System.out.println("                       You're reading. We're hiring. ");
        System.out.println("                       https://www.flickr.com/jobs/");
        System.out.println("");
        System.out.println("Simplified Lambda Example");
        System.out.println("=========================");
    }


}
