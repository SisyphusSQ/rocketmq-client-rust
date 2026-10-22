// Copyright 2023 The RocketMQ Rust Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#![allow(clippy::missing_const_for_thread_local)]
use std::cell::Cell;
use std::cell::RefCell;
use std::fmt;
use std::sync::Arc;

use rand::RngExt;
use rand::SeedableRng;

thread_local! {
    static THREAD_LOCAL_RNG: RefCell<rand::rngs::SmallRng> = RefCell::new(rand::rngs::SmallRng::seed_from_u64(rand::random()));
}

const POSITIVE_MASK: i32 = 0x7FFFFFFF;
const MAX: i32 = i32::MAX;

#[derive(Clone)]
pub struct ThreadLocalIndex {
    slots: Arc<thread_local::ThreadLocal<IndexSlot>>,
}

struct IndexSlot {
    thread: Cell<std::thread::ThreadId>,
    value: Cell<Option<i32>>,
}

impl ThreadLocalIndex {
    pub fn new() -> Self {
        Self {
            slots: Arc::new(thread_local::ThreadLocal::new()),
        }
    }

    pub fn increment_and_get(&self) -> i32 {
        let slot = self.current_slot();
        let random = || THREAD_LOCAL_RNG.with(|rng| rng.borrow_mut().random::<i32>());
        let new_value = next_java_index_value(slot.value.get(), random);
        slot.value.set(Some(new_value));
        new_value & POSITIVE_MASK
    }

    pub fn reset(&self) {
        self.current_slot()
            .value
            .set(Some(THREAD_LOCAL_RNG.with(|rng| rng.borrow_mut().random_range(0..MAX))));
    }

    fn current_slot(&self) -> &IndexSlot {
        let thread = std::thread::current().id();
        let slot = self.slots.get_or(|| IndexSlot {
            thread: Cell::new(thread),
            value: Cell::new(None),
        });
        // The library recycles storage IDs after thread exit. Preserve Java's
        // new-thread initialization rather than inheriting the retired thread's index.
        if slot.thread.get() != thread {
            slot.thread.set(thread);
            slot.value.set(None);
        }
        slot
    }

    /// Custom: retain Java's historical method name (the returned value is incremented).
    pub fn get_and_increment(&self) -> i32 {
        self.increment_and_get()
    }
}

impl Default for ThreadLocalIndex {
    fn default() -> Self {
        Self::new()
    }
}

#[inline]
fn next_java_index_value<F>(current: Option<i32>, mut random: F) -> i32
where
    F: FnMut() -> i32,
{
    // Custom: fixed Java 4.6.1 applies Math.abs at initialization and after
    // increment, then maps the unrepresentable abs(MIN_VALUE) to zero.
    let positive = |value: i32| value.wrapping_abs().max(0);
    positive(current.unwrap_or_else(|| positive(random())).wrapping_add(1))
}

impl fmt::Display for ThreadLocalIndex {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        {
            let index = self
                .slots
                .get()
                .filter(|slot| slot.thread.get() == std::thread::current().id())
                .and_then(|slot| slot.value.get());
            write!(
                f,
                "ThreadLocalIndex{{threadLocalIndex={}}}",
                index.map_or_else(|| "null".to_owned(), |v| v.to_string())
            )
        }
    }
}


