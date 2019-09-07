package edu.coursera.concurrent;

import edu.rice.pcdp.Actor;
import edu.rice.pcdp.PCDP;

/**
 * An actor-based implementation of the Sieve of Eratosthenes.
 * <p>
 * TODO Fill in the empty SieveActorActor actor class below and use it from
 * countPrimes to determin the number of primes <= limit.
 */
public final class SieveActor extends Sieve {
    /**
     * {@inheritDoc}
     * <p>
     * TODO Use the SieveActorActor class to calculate the number of primes <=
     * limit in parallel. You might consider how you can model the Sieve of
     * Eratosthenes as a pipeline of actors, each corresponding to a single
     * prime number.
     */

    @Override
    public int countPrimes(final int limit) {

        int numPrimes = 0;

        SieveActorActor actor = new SieveActorActor(2);

        PCDP.finish(() -> {
            for (int i = 3; i <= limit; i += 2) {
                actor.send(i);
            }
            actor.send(0);
        });

        SieveActorActor curr = actor;

        while (curr != null) {
            numPrimes += curr.getLocalPrimeCount();
            curr = curr.getNextActor();
        }

        return numPrimes;
    }

    /**
     * An actor class that helps implement the Sieve of Eratosthenes in
     * parallel.
     */
    public static final class SieveActorActor extends Actor {
        /**
         * Process a single message sent to this actor.
         * <p>
         * TODO complete this method.
         *
         * @param msg Received message
         */

        private final int MAX_LIMIT = 1000;
        private SieveActorActor nextActor;
        private int[] localPrimes;
        private int numLocalPrimes;

        private SieveActorActor(int num) {
            this.localPrimes = new int[MAX_LIMIT];
            this.localPrimes[0] = num;
            this.nextActor = null;
            this.numLocalPrimes = 1;
        }

        public int getLocalPrimeCount() {
            return this.numLocalPrimes;
        }

        public SieveActorActor getNextActor() {
            return this.nextActor;
        }


        @Override
        public void process(final Object msg) {
            int candidate = (Integer) msg;

            if (candidate <= 0) {
                if (nextActor != null) {
                    nextActor.send(0);
                }
            } else {
                boolean isLocalPrime = isLocalPrime(candidate);

                if (isLocalPrime) {
                    if (numLocalPrimes < MAX_LIMIT) {
                        this.localPrimes[numLocalPrimes] = candidate;
                        this.numLocalPrimes += 1;
                    } else if (nextActor == null) {
                        this.nextActor = new SieveActorActor(candidate);
                    } else {
                        this.nextActor.send(candidate);
                    }
                }
            }
        }

        private boolean isLocalPrime(int num) {
            for (int i = 0; i < this.numLocalPrimes; i++) {
                if (num % this.localPrimes[i] == 0) {
                    return false;
                }
            }
            return true;
        }
    }
}
