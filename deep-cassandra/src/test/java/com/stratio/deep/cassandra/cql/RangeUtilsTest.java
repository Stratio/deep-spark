/*
 * Copyright 2014, Stratio.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.stratio.deep.cassandra.cql;


import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.stratio.deep.commons.utils.Pair;
import javax.annotation.Nullable;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

@Test
public class RangeUtilsTest {

    private final Set<String> localTokens1 = Sets.newHashSet("-1818244161861818887", "-3328157778230026742",
            "-3343382513705824293", "-3410856225057464252", "-3596926862704751625", "-3656372461962736853",
            "-8365618907193383647", "-8778929611579079342", "-8871961696957088875", "-9084802855623558527",
            "2069541189440408621", "3833085318912712501", "4697951188745102648", "5003800885171690501",
            "6056649210961787737", "9045050270477118230");

    private final Set<String> remoteTokens1 = Sets.newHashSet("-1753979547465205696", "-225244981600479930",
            "-4277557647549019457", "-4667530077097108181", "-5048742246851688614", "-5953866357669553313",
            "-7239680221186626177", "-7420410787022862643", "-7426159207817454059", "-8780253142687646164",
            "2154538074560101335", "4673218458966404268", "503880126047823380", "6227498530632210420",
            "6296165828574267989", "840096810338067460");

    private final Set<String> localTokens2 = Sets.newHashSet(
            "-1008806316530991104", "-1044835113549955072", "-108086391056891904", "-1080863910568919040",
            "-1116892707587883008", "-1152921504606846976", "-1188950301625810944", "-1224979098644774912",
            "-1261007895663738880", "-1297036692682702848", "-1333065489701666816", "-1369094286720630784",
            "-1405123083739594752", "-144115188075855872", "-1441151880758558720", "-1477180677777522688",
            "-1513209474796486656", "-1549238271815450624", "-1585267068834414592", "-1621295865853378560",
            "-1657324662872342528", "-1693353459891306496", "-1729382256910270464", "-1765411053929234432",
            "-180143985094819840", "-1801439850948198400", "-1837468647967162368", "-1873497444986126336",
            "-1909526242005090304", "-1945555039024054272", "-1981583836043018240", "-2017612633061982208",
            "-2053641430080946176", "-2089670227099910144", "-2125699024118874112", "-216172782113783808",
            "-2161727821137838080", "-2197756618156802048", "-2233785415175766016", "-2269814212194729984",
            "-2305843009213693952", "-2341871806232657920", "-2377900603251621888", "-2413929400270585856",
            "-2449958197289549824", "-2485986994308513792", "-252201579132747776", "-2522015791327477760",
            "-2558044588346441728", "-2594073385365405696", "-2630102182384369664", "-2666130979403333632",
            "-2702159776422297600", "-2738188573441261568", "-2774217370460225536", "-2810246167479189504",
            "-2846274964498153472", "-288230376151711744", "-2882303761517117440", "-2918332558536081408",
            "-2954361355555045376", "-2990390152574009344", "-3026418949592973312", "-3062447746611937280",
            "-3098476543630901248", "-3134505340649865216", "-3170534137668829184", "-3206562934687793152",
            "-324259173170675712", "-3242591731706757120", "-3278620528725721088", "-3314649325744685056",
            "-3350678122763649024", "-3386706919782612992", "-3422735716801576960", "-3458764513820540928",
            "-3494793310839504896", "-3530822107858468864", "-3566850904877432832", "-36028797018963968",
            "-360287970189639680", "-3602879701896396800", "-3638908498915360768", "-3674937295934324736",
            "-3710966092953288704", "-3746994889972252672", "-3783023686991216640", "-3819052484010180608",
            "-3855081281029144576", "-3891110078048108544", "-3927138875067072512", "-396316767208603648",
            "-3963167672086036480", "-3999196469105000448", "-4035225266123964416", "-4071254063142928384",
            "-4107282860161892352", "-4143311657180856320", "-4179340454199820288", "-4215369251218784256",
            "-4251398048237748224", "-4287426845256712192", "-432345564227567616", "-4323455642275676160",
            "-4359484439294640128", "-4395513236313604096", "-4431542033332568064", "-4467570830351532032",
            "-4503599627370496000", "-4539628424389459968", "-4575657221408423936", "-4611686018427387904",
            "-4647714815446351872", "-468374361246531584", "-4683743612465315840", "-4719772409484279808",
            "-4755801206503243776", "-4791830003522207744", "-4827858800541171712", "-4863887597560135680",
            "-4899916394579099648", "-4935945191598063616", "-4971973988617027584", "-5008002785635991552",
            "-504403158265495552", "-5044031582654955520", "-5080060379673919488", "-5116089176692883456",
            "-5152117973711847424", "-5188146770730811392", "-5224175567749775360", "-5260204364768739328",
            "-5296233161787703296", "-5332261958806667264", "-5368290755825631232", "-540431955284459520",
            "-5404319552844595200", "-5440348349863559168", "-5476377146882523136", "-5512405943901487104",
            "-5548434740920451072", "-5584463537939415040", "-5620492334958379008", "-5656521131977342976",
            "-5692549928996306944", "-5728578726015270912", "-576460752303423488", "-5764607523034234880",
            "-5800636320053198848", "-5836665117072162816", "-5872693914091126784", "-5908722711110090752",
            "-5944751508129054720", "-5980780305148018688", "-6016809102166982656", "-6052837899185946624",
            "-6088866696204910592", "-612489549322387456", "-6124895493223874560", "-6160924290242838528",
            "-6196953087261802496", "-6232981884280766464", "-6269010681299730432", "-6305039478318694400",
            "-6341068275337658368", "-6377097072356622336", "-6413125869375586304", "-6449154666394550272",
            "-648518346341351424", "-6485183463413514240", "-6521212260432478208", "-6557241057451442176",
            "-6593269854470406144", "-6629298651489370112", "-6665327448508334080", "-6701356245527298048",
            "-6737385042546262016", "-6773413839565225984", "-6809442636584189952", "-684547143360315392",
            "-6845471433603153920", "-6881500230622117888", "-6917529027641081856", "-6953557824660045824",
            "-6989586621679009792", "-7025615418697973760", "-7061644215716937728", "-7097673012735901696",
            "-7133701809754865664", "-7169730606773829632", "-72057594037927936", "-720575940379279360",
            "-7205759403792793600", "-7241788200811757568", "-7277816997830721536", "-7313845794849685504",
            "-7349874591868649472", "-7385903388887613440", "-7421932185906577408", "-7457960982925541376",
            "-7493989779944505344", "-7530018576963469312", "-756604737398243328", "-7566047373982433280",
            "-7602076171001397248", "-7638104968020361216", "-7674133765039325184", "-7710162562058289152",
            "-7746191359077253120", "-7782220156096217088", "-7818248953115181056", "-7854277750134145024",
            "-7890306547153108992", "-792633534417207296", "-7926335344172072960", "-7962364141191036928",
            "-7998392938210000896", "-8034421735228964864", "-8070450532247928832", "-8106479329266892800",
            "-8142508126285856768", "-8178536923304820736", "-8214565720323784704", "-8250594517342748672",
            "-828662331436171264", "-8286623314361712640", "-8322652111380676608", "-8358680908399640576",
            "-8394709705418604544", "-8430738502437568512", "-8466767299456532480", "-8502796096475496448",
            "-8538824893494460416", "-8574853690513424384", "-8610882487532388352", "-864691128455135232",
            "-8646911284551352320", "-8682940081570316288", "-8718968878589280256", "-8754997675608244224",
            "-8791026472627208192", "-8827055269646172160", "-8863084066665136128", "-8899112863684100096",
            "-8935141660703064064", "-8971170457722028032", "-900719925474099200", "-9007199254740992000",
            "-9043228051759955968", "-9079256848778919936", "-9115285645797883904", "-9151314442816847872",
            "-9187343239835811840", "-936748722493063168", "-972777519512027136"
    );

    private final Set<String> remoteTokens2 = Sets.newHashSet(
            "-9223372036854775808", "1008806316530991103", "1044835113549955071", "108086391056891903",
            "1080863910568919039", "1116892707587883007", "1152921504606846975", "1188950301625810943",
            "1224979098644774911", "1261007895663738879", "1297036692682702847", "1333065489701666815",
            "1369094286720630783", "1405123083739594751", "144115188075855871", "1441151880758558719",
            "1477180677777522687", "1513209474796486655", "1549238271815450623", "1585267068834414591",
            "1621295865853378559", "1657324662872342527", "1693353459891306495", "1729382256910270463",
            "1765411053929234431", "180143985094819839", "1801439850948198399", "1837468647967162367",
            "1873497444986126335", "1909526242005090303", "1945555039024054271", "1981583836043018239",
            "2017612633061982207", "2053641430080946175", "2089670227099910143", "2125699024118874111",
            "216172782113783807", "2161727821137838079", "2197756618156802047", "2233785415175766015",
            "2269814212194729983", "2305843009213693951", "2341871806232657919", "2377900603251621887",
            "2413929400270585855", "2449958197289549823", "2485986994308513791", "252201579132747775",
            "2522015791327477759", "2558044588346441727", "2594073385365405695", "2630102182384369663",
            "2666130979403333631", "2702159776422297599", "2738188573441261567", "2774217370460225535",
            "2810246167479189503", "2846274964498153471", "288230376151711743", "2882303761517117439",
            "2918332558536081407", "2954361355555045375", "2990390152574009343", "3026418949592973311",
            "3062447746611937279", "3098476543630901247", "3134505340649865215", "3170534137668829183",
            "3206562934687793151", "324259173170675711", "3242591731706757119", "3278620528725721087",
            "3314649325744685055", "3350678122763649023", "3386706919782612991", "3422735716801576959",
            "3458764513820540927", "3494793310839504895", "3530822107858468863", "3566850904877432831",
            "36028797018963967", "360287970189639679", "3602879701896396799", "3638908498915360767",
            "3674937295934324735", "3710966092953288703", "3746994889972252671", "3783023686991216639",
            "3819052484010180607", "3855081281029144575", "3891110078048108543", "3927138875067072511",
            "396316767208603647", "3963167672086036479", "3999196469105000447", "4035225266123964415",
            "4071254063142928383", "4107282860161892351", "4143311657180856319", "4179340454199820287",
            "4215369251218784255", "4251398048237748223", "4287426845256712191", "432345564227567615",
            "4323455642275676159", "4359484439294640127", "4395513236313604095", "4431542033332568063",
            "4467570830351532031", "4503599627370495999", "4539628424389459967", "4575657221408423935",
            "4611686018427387903", "4647714815446351871", "468374361246531583", "4683743612465315839",
            "4719772409484279807", "4755801206503243775", "4791830003522207743", "4827858800541171711",
            "4863887597560135679", "4899916394579099647", "4935945191598063615", "4971973988617027583",
            "5008002785635991551", "504403158265495551", "5044031582654955519", "5080060379673919487",
            "5116089176692883455", "5152117973711847423", "5188146770730811391", "5224175567749775359",
            "5260204364768739327", "5296233161787703295", "5332261958806667263", "5368290755825631231",
            "540431955284459519", "5404319552844595199", "5440348349863559167", "5476377146882523135",
            "5512405943901487103", "5548434740920451071", "5584463537939415039", "5620492334958379007",
            "5656521131977342975", "5692549928996306943", "5728578726015270911", "576460752303423487",
            "5764607523034234879", "5800636320053198847", "5836665117072162815", "5872693914091126783",
            "5908722711110090751", "5944751508129054719", "5980780305148018687", "6016809102166982655",
            "6052837899185946623", "6088866696204910591", "612489549322387455", "6124895493223874559",
            "6160924290242838527", "6196953087261802495", "6232981884280766463", "6269010681299730431",
            "6305039478318694399", "6341068275337658367", "6377097072356622335", "6413125869375586303",
            "6449154666394550271", "648518346341351423", "6485183463413514239", "6521212260432478207",
            "6557241057451442175", "6593269854470406143", "6629298651489370111", "6665327448508334079",
            "6701356245527298047", "6737385042546262015", "6773413839565225983", "6809442636584189951",
            "684547143360315391", "6845471433603153919", "6881500230622117887", "6917529027641081855",
            "6953557824660045823", "6989586621679009791", "7025615418697973759", "7061644215716937727",
            "7097673012735901695", "7133701809754865663", "7169730606773829631", "72057594037927935",
            "720575940379279359", "7205759403792793599", "7241788200811757567", "7277816997830721535",
            "7313845794849685503", "7349874591868649471", "7385903388887613439", "7421932185906577407",
            "7457960982925541375", "7493989779944505343", "7530018576963469311", "756604737398243327",
            "7566047373982433279", "7602076171001397247", "7638104968020361215", "7674133765039325183",
            "7710162562058289151", "7746191359077253119", "7782220156096217087", "7818248953115181055",
            "7854277750134145023", "7890306547153108991", "792633534417207295", "7926335344172072959",
            "7962364141191036927", "7998392938210000895", "8034421735228964863", "8070450532247928831",
            "8106479329266892799", "8142508126285856767", "8178536923304820735", "8214565720323784703",
            "8250594517342748671", "828662331436171263", "8286623314361712639", "8322652111380676607",
            "8358680908399640575", "8394709705418604543", "8430738502437568511", "8466767299456532479",
            "8502796096475496447", "8538824893494460415", "8574853690513424383", "8610882487532388351",
            "864691128455135231", "8646911284551352319", "8682940081570316287", "8718968878589280255",
            "8754997675608244223", "8791026472627208191", "8827055269646172159", "8863084066665136127",
            "8899112863684100095", "8935141660703064063", "8971170457722028031", "900719925474099199",
            "9007199254740991999", "9043228051759955967", "9079256848778919935", "9115285645797883903",
            "9151314442816847871", "9187343239835811839", "936748722493063167", "972777519512027135"
    );

	private final Set<String> localTokens3 = Sets.newHashSet("-6148914691236517206");

	private final Set<String> remoteTokens3 = Sets.newHashSet("-9223372036854775808",
					"-3074457345618258604", "-2", "3074457345618258600", "6148914691236517202");

    @Mock
    private Session mockSession1;

    @Mock
    private ResultSet mockLocalTokensResultSet1;

    @Mock
    private Session mockSession2;

    @Mock
    private ResultSet mockLocalTokensResultSet2;

	@Mock
	private Session mockSession3;

	@Mock
	private ResultSet mockLocalTokensResultSet3;


    @BeforeMethod
    protected void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);

        /* mock example 1 */
        List<Row> rows = new ArrayList<>();

        Row localTokensRow = mock(Row.class);
        when(localTokensRow.getInet(any(String.class))).thenThrow(IllegalArgumentException.class);
        Set<String> shuffledLocalTokens = Sets.newHashSet(Ordering.arbitrary().sortedCopy(localTokens1));
        when(localTokensRow.getSet("tokens", String.class)).thenReturn(shuffledLocalTokens);
        rows.add(localTokensRow);

        Row remoteTokensRow = mock(Row.class);
        InetAddress mockInet = mock(InetAddress.class);
        when(mockInet.getHostName()).thenReturn("fake-hostname");
        when(remoteTokensRow.getInet("peer")).thenReturn(mockInet);

        Set<String> shuffledRemoteTokens = Sets.newHashSet(Ordering.arbitrary().sortedCopy(remoteTokens1));
        when(remoteTokensRow.getSet("tokens", String.class)).thenReturn(shuffledRemoteTokens);
        rows.add(remoteTokensRow);

        when(mockLocalTokensResultSet1.all()).thenReturn(rows);
        when(mockSession1.execute(any(String.class))).thenReturn(mockLocalTokensResultSet1);

        /* mock example 2 */
        List<Row> rows2 = new ArrayList<>();
        Row localTokensRow2 = mock(Row.class);
        when(localTokensRow2.getInet(any(String.class))).thenThrow(IllegalArgumentException.class);
        Set<String> shuffledLocalTokens2 = Sets.newHashSet(Ordering.arbitrary().sortedCopy(localTokens2));
        when(localTokensRow2.getSet("tokens", String.class)).thenReturn(shuffledLocalTokens2);
        rows2.add(localTokensRow2);

        Row remoteTokensRow2 = mock(Row.class);
        when(remoteTokensRow2.getInet("peer")).thenReturn(mockInet);

        Set<String> shuffledRemoteTokens2 = Sets.newHashSet(Ordering.arbitrary().sortedCopy(remoteTokens2));
        when(remoteTokensRow2.getSet("tokens", String.class)).thenReturn(shuffledRemoteTokens2);
        rows2.add(remoteTokensRow2);

        when(mockLocalTokensResultSet2.all()).thenReturn(rows2);
        when(mockSession2.execute(any(String.class))).thenReturn(mockLocalTokensResultSet2);

	    /* mock example 3 */
	    List<Row> rows3 = new ArrayList<>();
	    Row localTokensRow3 = mock(Row.class);
	    when(localTokensRow3.getInet(any(String.class))).thenThrow(IllegalArgumentException.class);
	    Set<String> shuffledLocalTokens3 = Sets.newHashSet(Ordering.arbitrary().sortedCopy(localTokens3));
	    when(localTokensRow3.getSet("tokens", String.class)).thenReturn(shuffledLocalTokens3);
	    rows3.add(localTokensRow3);

	    Row remoteTokensRow3 = mock(Row.class);
	    when(remoteTokensRow3.getInet("peer")).thenReturn(mockInet);

	    Set<String> shuffledRemoteTokens3 = Sets.newHashSet(Ordering.arbitrary().sortedCopy(remoteTokens3));
	    when(remoteTokensRow3.getSet("tokens", String.class)).thenReturn(shuffledRemoteTokens3);
	    rows3.add(remoteTokensRow3);

	    when(mockLocalTokensResultSet3.all()).thenReturn(rows3);
	    when(mockSession3.getLoggedKeyspace()).thenReturn("fake-keyspace");
	    when(mockSession3.execute(any(String.class))).thenReturn(mockLocalTokensResultSet3);
    }

    @Test
    public void testFetchSortedTokens1() {

        Map<String, Iterable<Comparable>> sortedTokens =
                RangeUtils.fetchTokens("none", Pair.create(mockSession1, "localhost"), new Murmur3Partitioner());

        assertEquals(sortedTokens.size(), 2);

        List<Comparable> localTokens = Lists.newArrayList(sortedTokens.get("localhost"));
        assertNotNull(localTokens);
        assertEquals(localTokens.size(), 16);

        Iterable sortedLocalTokens1 = Ordering.natural().immutableSortedCopy(Iterables.transform(localTokens1,
                new Function<String, Long>() {
                    @Nullable
                    @Override
                    public Long apply(@Nullable String input) {
                        return Long.parseLong(input);
                    }
                }));
        boolean elementsEquals = Iterables.elementsEqual(
                Ordering.natural().sortedCopy(localTokens),
                sortedLocalTokens1);

        assertTrue(elementsEquals);

        List<Comparable> remoteTokens = Lists.newArrayList(sortedTokens.get("fake-hostname"));
        assertNotNull(remoteTokens);
        assertEquals(remoteTokens.size(), 16);

        Iterable sortedRemoteTokens1 = Ordering.natural().immutableSortedCopy(Iterables.transform(remoteTokens1,
                new Function<String, Long>() {
                    @Nullable
                    @Override
                    public Long apply(@Nullable String input) {
                        return Long.parseLong(input);
                    }
                }));

        elementsEquals = Iterables.elementsEqual(
                Ordering.natural().sortedCopy(sortedRemoteTokens1),
                Ordering.natural().sortedCopy(remoteTokens));

        assertTrue(elementsEquals);
    }

    @Test
    public void testFetchSortedTokens2() {

        Map<String, Iterable<Comparable>> sortedTokens =
                RangeUtils.fetchTokens("none", Pair.create(mockSession2, "localhost"), new Murmur3Partitioner());

        assertEquals(sortedTokens.size(), 2);

        List<Comparable> localTokens = Lists.newArrayList(sortedTokens.get("localhost"));
        assertNotNull(localTokens);
        assertEquals(localTokens.size(), 255);

        Iterable sortedLocalTokens1 = Ordering.natural().immutableSortedCopy(Iterables.transform(localTokens2,
                new Function<String, Long>() {
                    @Nullable
                    @Override
                    public Long apply(@Nullable String input) {
                        return Long.parseLong(input);
                    }
                }));
        boolean elementsEquals = Iterables.elementsEqual(
                Ordering.natural().sortedCopy(localTokens),
                sortedLocalTokens1);

        assertTrue(elementsEquals);

        List<Comparable> remoteTokens = Lists.newArrayList(sortedTokens.get("fake-hostname"));
        assertNotNull(remoteTokens);
        assertEquals(remoteTokens.size(), 256);

        Iterable sortedRemoteTokens1 = Ordering.natural().immutableSortedCopy(Iterables.transform(remoteTokens2,
                new Function<String, Long>() {
                    @Nullable
                    @Override
                    public Long apply(@Nullable String input) {
                        return Long.parseLong(input);
                    }
                }));

        elementsEquals = Iterables.elementsEqual(
                Ordering.natural().sortedCopy(sortedRemoteTokens1),
                Ordering.natural().sortedCopy(remoteTokens));

        assertTrue(elementsEquals);
    }

}
